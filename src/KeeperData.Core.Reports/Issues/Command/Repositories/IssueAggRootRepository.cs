using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Internal.Collections;
using KeeperData.Core.Reports.Internal.Documents;
using KeeperData.Core.Reports.Internal.Mappers;
using KeeperData.Core.Reports.Issues.Command.Abstract;
using KeeperData.Core.Reports.Issues.Command.AggregateRoots;
using KeeperData.Core.Reports.Issues.Index;
using KeeperData.Core.Reports.Operations;
using KeeperData.Core.Throttling;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace KeeperData.Core.Reports.Issues.Command.Repositories;

/// <summary>
/// MongoDB repository for Issue aggregate root persistence.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB repository - covered by integration tests.")]
public class IssueAggRootRepository(IssueCollection issueCollection,
    IIssueIndexManager issueIndexManager,
    IThrottler throttler,
    ILogger<IssueAggRootRepository> logger) : IIssueAggRootRepository
{
    private readonly IMongoCollection<IssueDocument> _collection = issueCollection.Collection;

    public async Task<Issue?> GetByIdAsync(string id, CancellationToken ct = default)
    {
        var filter = Builders<IssueDocument>.Filter.Eq(d => d.Id, id);
        var document = await _collection.Find(filter).FirstOrDefaultAsync(ct);
        return document?.ToAggregateRoot();
    }

    public async Task UpsertAsync(Issue item, CancellationToken ct = default)
    {
        var filter = Builders<IssueDocument>.Filter.Eq(d => d.Id, item.Id);
        var options = new ReplaceOptions { IsUpsert = true };
        await _collection.ReplaceOneAsync(filter, item.ToDocument(), options, ct);
    }

    public async Task<int> DeactivateStaleAsync(string currentOperationId, Func<int, int, Task>? onBatchProcessed, CancellationToken ct = default, OperationScope? scope = null, Func<bool>? isCancellationRequested = null)
    {
        logger.LogInformation("Deactivating stale issues: starting (OperationId={OperationId})", currentOperationId);
        var stopwatch = Stopwatch.StartNew();

        var staleFilter = Builders<IssueDocument>.Filter.And(
            Builders<IssueDocument>.Filter.Eq(d => d.IsActive, true),
            Builders<IssueDocument>.Filter.Ne(d => d.OperationId, currentOperationId));

        var (totalStale, countMs) = await Timed.RunAsync(async () =>
            (int)await _collection.CountDocumentsAsync(staleFilter, cancellationToken: ct));
        scope?.TrackElapsed("counting", countMs);
        scope?.Start(totalStale, $"Deactivating {totalStale} stale issues");

        var totalDeactivated = 0;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var settings = throttler.Settings.IssueDeactivation;

                // Find a batch of stale document IDs (lightweight read, _id only)
                var (staleIds, fetchMs) = await Timed.RunAsync(() => _collection
                    .Find(staleFilter)
                    .Project(d => d.Id)
                    .Limit(settings.BatchSize)
                    .ToListAsync(ct));
                scope?.TrackElapsed("batch_fetch", fetchMs);

                if (staleIds.Count == 0)
                {
                    break;
                }

                // Update this batch using an indexed _id $in filter
                var (result, updateMs) = await Timed.RunAsync(async () =>
                {
                    var batchFilter = Builders<IssueDocument>.Filter.In(d => d.Id, staleIds);
                    var update = Builders<IssueDocument>.Update
                        .Set(d => d.IsActive, false)
                        .Set(d => d.LastUpdatedAtUtc, DateTime.UtcNow);
                    return await _collection.UpdateManyAsync(batchFilter, update, cancellationToken: ct);
                });
                scope?.TrackElapsed("batch_update", updateMs);
                totalDeactivated += (int)result.ModifiedCount;
                scope?.UpdateProgress(totalDeactivated, $"Deactivated {totalDeactivated} of {totalStale} stale issues");

                if (onBatchProcessed is not null)
                {
                    await onBatchProcessed(totalDeactivated, totalStale);
                }

                if (staleIds.Count < settings.BatchSize)
                {
                    break;
                }

                var delayMs = await Timed.RunAsync(() => throttler.DelayAsync(settings.ThrottleDelayMs, ct));
                scope?.TrackElapsed("throttle_wait", delayMs);

                if (isCancellationRequested?.Invoke() == true)
                    throw new OperationCanceledException("Cancellation requested via progress tracker.");
            }

            scope?.Complete($"Deactivated {totalDeactivated} stale issues");
        }
        catch (OperationCanceledException) { scope?.Cancel("Deactivation cancelled"); throw; }
        catch (Exception ex) { scope?.Fail(ex.Message); throw; }

        stopwatch.Stop();
        logger.LogInformation("Deactivating stale issues: completed. Deactivated={DeactivatedCount}, Duration={DurationMs}ms ({DurationSeconds}s)",
            totalDeactivated, stopwatch.ElapsedMilliseconds, stopwatch.Elapsed.TotalSeconds);

        return totalDeactivated;
    }

    public async Task<long> DeleteAllAsync(CancellationToken ct = default)
    {
        var countBefore = await _collection.CountDocumentsAsync(Builders<IssueDocument>.Filter.Empty, cancellationToken: ct);
        await _collection.Database.DropCollectionAsync(_collection.CollectionNamespace.CollectionName, ct);
        await issueIndexManager.ForceRecreateIndexesAsync(ct);
        return countBefore;
    }
}
