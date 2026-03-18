using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Internal.Collections;
using KeeperData.Core.Reports.Internal.Documents;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace KeeperData.Core.Reports.Issues.Index;

/// <summary>
/// Service responsible for ensuring issue history collection indexes exist.
/// Follows Single Responsibility Principle - only handles index creation.
/// </summary>
public interface IIssueHistoryIndexManager
{
    /// <summary>
    /// Ensures all required indexes exist on the issue history collection.
    /// Idempotent - safe to call multiple times.
    /// </summary>
    Task EnsureIndexesAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Default implementation of issue history index management.
/// Creates indexes optimized for history lookup by issue ID.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB index manager - covered by integration tests.")]
public class IssueHistoryIndexManager : IIssueHistoryIndexManager
{
    private readonly IMongoCollection<IssueHistoryEntryDocument> _collection;
    private readonly ILogger<IssueHistoryIndexManager> _logger;
    private bool _indexesCreated;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    public IssueHistoryIndexManager(
        IssueHistoryCollection issueHistoryCollection,
        ILogger<IssueHistoryIndexManager> logger)
    {
        _collection = issueHistoryCollection.Collection;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task EnsureIndexesAsync(CancellationToken cancellationToken = default)
    {
        if (_indexesCreated) return;

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_indexesCreated) return;

            _logger.LogInformation("Creating issue history collection indexes...");

            await CreateIssueIdOccurredAtIndexAsync(cancellationToken);

            _indexesCreated = true;
            _logger.LogInformation("Issue history collection indexes created successfully");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to create issue history collection indexes. Continuing anyway.");
        }
        finally
        {
            _initLock.Release();
        }
    }

    /// <summary>
    /// Compound index for history lookup: filter by issue_id, sorted by occurred_at.
    /// Query pattern: WHERE issue_id = X ORDER BY occurred_at ASC
    /// </summary>
    private async Task CreateIssueIdOccurredAtIndexAsync(CancellationToken cancellationToken)
    {
        var indexKeys = Builders<IssueHistoryEntryDocument>.IndexKeys
            .Ascending(d => d.IssueId)
            .Ascending(d => d.OccurredAtUtc);

        var indexModel = new CreateIndexModel<IssueHistoryEntryDocument>(
            indexKeys,
            new CreateIndexOptions
            {
                Name = "idx_issue_history_issueid_occurredat",
                Background = true
            });

        await _collection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created index on (issue_id, occurred_at)");
    }
}
