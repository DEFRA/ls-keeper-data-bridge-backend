using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Internal.Collections;
using KeeperData.Core.Reports.Internal.Documents;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace KeeperData.Core.Reports.Cleanse.Export.Index;

/// <summary>
/// Service responsible for ensuring cleanse export operations collection indexes exist.
/// </summary>
public interface ICleanseExportOperationsIndexManager
{
    Task EnsureIndexesAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Default implementation of cleanse export operations index management.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB index manager - covered by integration tests.")]
public class CleanseExportOperationsIndexManager : ICleanseExportOperationsIndexManager
{
    private readonly IMongoCollection<CleanseExportOperationDocument> _collection;
    private readonly ILogger<CleanseExportOperationsIndexManager> _logger;
    private bool _indexesCreated;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    public CleanseExportOperationsIndexManager(
        CleanseExportOperationsCollection collection,
        ILogger<CleanseExportOperationsIndexManager> logger)
    {
        _collection = collection.Collection;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task EnsureIndexesAsync(CancellationToken cancellationToken = default)
    {
        if (_indexesCreated) return;

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_indexesCreated) return;

            _logger.LogInformation("Creating cleanse export operations collection indexes...");

            await CreateStartedAtIndexAsync(cancellationToken);
            await CreateStatusIndexAsync(cancellationToken);

            _indexesCreated = true;
            _logger.LogInformation("Cleanse export operations collection indexes created successfully");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to create cleanse export operations collection indexes. Continuing anyway.");
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task CreateStartedAtIndexAsync(CancellationToken cancellationToken)
    {
        var indexKeys = Builders<CleanseExportOperationDocument>.IndexKeys
            .Descending(d => d.StartedAtUtc);

        var indexModel = new CreateIndexModel<CleanseExportOperationDocument>(
            indexKeys,
            new CreateIndexOptions
            {
                Name = "idx_exports_started_at",
                Background = true
            });

        await _collection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created index on (started_at_utc DESC)");
    }

    private async Task CreateStatusIndexAsync(CancellationToken cancellationToken)
    {
        var indexKeys = Builders<CleanseExportOperationDocument>.IndexKeys
            .Ascending(d => d.Status);

        var indexModel = new CreateIndexModel<CleanseExportOperationDocument>(
            indexKeys,
            new CreateIndexOptions
            {
                Name = "idx_exports_status",
                Background = true
            });

        await _collection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created index on (status)");
    }
}
