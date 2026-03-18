using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Internal.Collections;
using KeeperData.Core.Reports.Internal.Documents;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace KeeperData.Core.Reports.Cleanse.Operations.Index;

/// <summary>
/// Service responsible for ensuring cleanse operations collection indexes exist.
/// Follows Single Responsibility Principle - only handles index creation.
/// </summary>
public interface ICleanseOperationsIndexManager
{
    /// <summary>
    /// Ensures all required indexes exist on the cleanse operations collection.
    /// Idempotent - safe to call multiple times.
    /// </summary>
    Task EnsureIndexesAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Default implementation of cleanse operations index management.
/// Creates indexes optimized for operations listing and status queries.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB index manager - covered by integration tests.")]
public class CleanseOperationsIndexManager : ICleanseOperationsIndexManager
{
    private readonly IMongoCollection<CleanseAnalysisOperationDocument> _collection;
    private readonly ILogger<CleanseOperationsIndexManager> _logger;
    private bool _indexesCreated;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    public CleanseOperationsIndexManager(
        CleanseOperationsCollection operationsCollection,
        ILogger<CleanseOperationsIndexManager> logger)
    {
        _collection = operationsCollection.Collection;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task EnsureIndexesAsync(CancellationToken cancellationToken = default)
    {
        if (_indexesCreated) return;

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_indexesCreated) return;

            _logger.LogInformation("Creating cleanse operations collection indexes...");

            await CreateStartedAtIndexAsync(cancellationToken);
            await CreateStatusIndexAsync(cancellationToken);

            _indexesCreated = true;
            _logger.LogInformation("Cleanse operations collection indexes created successfully");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to create cleanse operations collection indexes. Continuing anyway.");
        }
        finally
        {
            _initLock.Release();
        }
    }

    /// <summary>
    /// Descending index on started_at_utc for operations listing sorted by most recent.
    /// Query pattern: ORDER BY started_at_utc DESC with SKIP/LIMIT
    /// </summary>
    private async Task CreateStartedAtIndexAsync(CancellationToken cancellationToken)
    {
        var indexKeys = Builders<CleanseAnalysisOperationDocument>.IndexKeys
            .Descending(d => d.StartedAtUtc);

        var indexModel = new CreateIndexModel<CleanseAnalysisOperationDocument>(
            indexKeys,
            new CreateIndexOptions
            {
                Name = "idx_operations_started_at",
                Background = true
            });

        await _collection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created index on (started_at_utc DESC)");
    }

    /// <summary>
    /// Single-field index on status for finding the current running operation.
    /// Query pattern: WHERE status = "Running"
    /// </summary>
    private async Task CreateStatusIndexAsync(CancellationToken cancellationToken)
    {
        var indexKeys = Builders<CleanseAnalysisOperationDocument>.IndexKeys
            .Ascending(d => d.Status);

        var indexModel = new CreateIndexModel<CleanseAnalysisOperationDocument>(
            indexKeys,
            new CreateIndexOptions
            {
                Name = "idx_operations_status",
                Background = true
            });

        await _collection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created index on (status)");
    }
}
