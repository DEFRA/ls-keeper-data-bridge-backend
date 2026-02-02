using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reporting.Domain;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace KeeperData.Core.Reporting.Services;

/// <summary>
/// Service responsible for ensuring lineage collection indexes exist.
/// Follows Single Responsibility Principle - only handles index creation.
/// </summary>
public interface ILineageIndexManager
{
    /// <summary>
    /// Ensures all required indexes exist on the lineage events collection.
    /// Idempotent - safe to call multiple times.
    /// </summary>
    Task EnsureIndexesAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Default implementation of lineage index management.
/// Creates compound indexes optimized for primary query patterns.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB index manager - covered by integration tests.")]
public class LineageIndexManager : ILineageIndexManager
{
    private readonly IMongoCollection<LineageEventDocument> _lineageEventsCollection;
    private readonly ILogger<LineageIndexManager> _logger;
    private bool _indexesCreated;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    public LineageIndexManager(
        IMongoCollection<LineageEventDocument> lineageEventsCollection,
        ILogger<LineageIndexManager> logger)
    {
        _lineageEventsCollection = lineageEventsCollection ?? throw new ArgumentNullException(nameof(lineageEventsCollection));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task EnsureIndexesAsync(CancellationToken cancellationToken = default)
    {
        if (_indexesCreated) return;

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_indexesCreated) return;

            _logger.LogInformation("Creating lineage event indexes...");

            await CreatePrimaryIndexAsync(cancellationToken);
            await CreateCollectionDateIndexAsync(cancellationToken);
            await CreateImportIndexAsync(cancellationToken);

            _indexesCreated = true;
            _logger.LogInformation("Lineage event indexes created successfully");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to create lineage event indexes. Continuing anyway.");
            // Don't throw - indexes are performance optimization, not required for functionality
        }
        finally
        {
            _initLock.Release();
        }
    }

    /// <summary>
    /// Primary index: Get all events for a specific record, ordered chronologically.
    /// Query pattern: WHERE LineageDocumentId = X ORDER BY EventDateUtc
    /// </summary>
    private async Task CreatePrimaryIndexAsync(CancellationToken cancellationToken)
    {
        var indexKeys = Builders<LineageEventDocument>.IndexKeys
            .Ascending(x => x.LineageDocumentId)
            .Ascending(x => x.EventDateUtc);

        var indexModel = new CreateIndexModel<LineageEventDocument>(
            indexKeys,
            new CreateIndexOptions
            {
                Name = "idx_lineage_events_primary",
                Background = true
            });

        await _lineageEventsCollection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created primary index on (LineageDocumentId, EventDateUtc)");
    }

    /// <summary>
    /// Secondary index: Get events by collection + date range.
    /// Query pattern: WHERE CollectionName = X AND EventDateUtc BETWEEN Y AND Z
    /// </summary>
    private async Task CreateCollectionDateIndexAsync(CancellationToken cancellationToken)
    {
        var indexKeys = Builders<LineageEventDocument>.IndexKeys
            .Ascending(x => x.CollectionName)
            .Ascending(x => x.EventDateUtc);

        var indexModel = new CreateIndexModel<LineageEventDocument>(
            indexKeys,
            new CreateIndexOptions
            {
                Name = "idx_lineage_events_collection_date",
                Background = true
            });

        await _lineageEventsCollection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created collection-date index on (CollectionName, EventDateUtc)");
    }

    /// <summary>
    /// Tertiary index: Get events by import.
    /// Query pattern: WHERE ImportId = X
    /// </summary>
    private async Task CreateImportIndexAsync(CancellationToken cancellationToken)
    {
        var indexKeys = Builders<LineageEventDocument>.IndexKeys
            .Ascending(x => x.ImportId);

        var indexModel = new CreateIndexModel<LineageEventDocument>(
            indexKeys,
            new CreateIndexOptions
            {
                Name = "idx_lineage_events_import",
                Background = true
            });

        await _lineageEventsCollection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created import index on (ImportId)");
    }
}