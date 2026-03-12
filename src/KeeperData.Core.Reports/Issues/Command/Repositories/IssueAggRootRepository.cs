using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Internal.Collections;
using KeeperData.Core.Reports.Internal.Documents;
using KeeperData.Core.Reports.Internal.Mappers;
using KeeperData.Core.Reports.Issues.Command.Abstract;
using KeeperData.Core.Reports.Issues.Command.AggregateRoots;
using KeeperData.Core.Throttling;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace KeeperData.Core.Reports.Issues.Command.Repositories;

/// <summary>
/// MongoDB repository for Issue aggregate root persistence.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB repository - covered by integration tests.")]
public class IssueAggRootRepository(IssueCollection issueCollection,
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

    public async Task<int> DeactivateStaleAsync(string currentOperationId, Func<int, int, Task>? onBatchProcessed, CancellationToken ct = default)
    {
        logger.LogInformation("Deactivating stale issues: starting (OperationId={OperationId})", currentOperationId);
        var stopwatch = Stopwatch.StartNew();

        var staleFilter = Builders<IssueDocument>.Filter.And(
            Builders<IssueDocument>.Filter.Eq(d => d.IsActive, true),
            Builders<IssueDocument>.Filter.Ne(d => d.OperationId, currentOperationId));

        var totalStale = (int)await _collection.CountDocumentsAsync(staleFilter, cancellationToken: ct);
        var totalDeactivated = 0;

        while (!ct.IsCancellationRequested)
        {
            var settings = throttler.Settings.IssueDeactivation;

            // Find a batch of stale document IDs (lightweight read, _id only)
            var staleIds = await _collection
                .Find(staleFilter)
                .Project(d => d.Id)
                .Limit(settings.BatchSize)
                .ToListAsync(ct);

            if (staleIds.Count == 0)
            {
                break;
            }

            // Update this batch using an indexed _id $in filter
            var batchFilter = Builders<IssueDocument>.Filter.In(d => d.Id, staleIds);
            var update = Builders<IssueDocument>.Update
                .Set(d => d.IsActive, false)
                .Set(d => d.LastUpdatedAtUtc, DateTime.UtcNow);

            var result = await _collection.UpdateManyAsync(batchFilter, update, cancellationToken: ct);
            totalDeactivated += (int)result.ModifiedCount;

            if (onBatchProcessed is not null)
            {
                await onBatchProcessed(totalDeactivated, totalStale);
            }

            if (staleIds.Count < settings.BatchSize)
            {
                break;
            }

            await throttler.DelayAsync(settings.ThrottleDelayMs, ct);
        }

        stopwatch.Stop();
        logger.LogInformation("Deactivating stale issues: completed. Deactivated={DeactivatedCount}, Duration={DurationMs}ms ({DurationSeconds}s)",
            totalDeactivated, stopwatch.ElapsedMilliseconds, stopwatch.Elapsed.TotalSeconds);

        return totalDeactivated;
    }

    public async Task<long> DeleteAllAsync(CancellationToken ct = default)
    {
        var result = await _collection.DeleteManyAsync(Builders<IssueDocument>.Filter.Empty, ct);
        return result.DeletedCount;
    }
}
