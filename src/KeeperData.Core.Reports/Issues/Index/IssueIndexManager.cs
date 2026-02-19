using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Internal.Collections;
using KeeperData.Core.Reports.Internal.Documents;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace KeeperData.Core.Reports.Issues.Index;

/// <summary>
/// Service responsible for ensuring issue collection indexes exist.
/// Follows Single Responsibility Principle - only handles index creation.
/// </summary>
public interface IIssueIndexManager
{
    /// <summary>
    /// Ensures all required indexes exist on the issues collection.
    /// Idempotent - safe to call multiple times.
    /// </summary>
    Task EnsureIndexesAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Default implementation of issue index management.
/// Creates compound indexes optimized for export and query patterns.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB index manager - covered by integration tests.")]
public class IssueIndexManager : IIssueIndexManager
{
    private readonly IMongoCollection<IssueDocument> _collection;
    private readonly ILogger<IssueIndexManager> _logger;
    private bool _indexesCreated;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    public IssueIndexManager(
        IssueCollection issueCollection,
        ILogger<IssueIndexManager> logger)
    {
        _collection = issueCollection.Collection;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task EnsureIndexesAsync(CancellationToken cancellationToken = default)
    {
        if (_indexesCreated) return;

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_indexesCreated) return;

            _logger.LogInformation("Creating issue collection indexes...");

            await CreateExportSortIndexAsync(cancellationToken);
            await CreateActiveIssueCodeIndexAsync(cancellationToken);

            _indexesCreated = true;
            _logger.LogInformation("Issue collection indexes created successfully");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to create issue collection indexes. Continuing anyway.");
        }
        finally
        {
            _initLock.Release();
        }
    }

    /// <summary>
    /// Compound index for export streaming: filter active issues by issue_code, sorted by CPH.
    /// Query pattern: WHERE is_active = true AND issue_code = X ORDER BY cph ASC
    /// </summary>
    private async Task CreateExportSortIndexAsync(CancellationToken cancellationToken)
    {
        var indexKeys = Builders<IssueDocument>.IndexKeys
            .Ascending(d => d.IsActive)
            .Ascending(d => d.IssueCode)
            .Ascending(d => d.Cph);

        var indexModel = new CreateIndexModel<IssueDocument>(
            indexKeys,
            new CreateIndexOptions
            {
                Name = "idx_issues_active_issuecode_cph",
                Background = true
            });

        await _collection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created export sort index on (is_active, issue_code, cph)");
    }

    /// <summary>
    /// Compound index for general active issue queries by issue code.
    /// Query pattern: WHERE is_active = true AND issue_code = X
    /// </summary>
    private async Task CreateActiveIssueCodeIndexAsync(CancellationToken cancellationToken)
    {
        var indexKeys = Builders<IssueDocument>.IndexKeys
            .Ascending(d => d.IsActive)
            .Ascending(d => d.IssueCode);

        var indexModel = new CreateIndexModel<IssueDocument>(
            indexKeys,
            new CreateIndexOptions
            {
                Name = "idx_issues_active_issuecode",
                Background = true
            });

        await _collection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created active issue code index on (is_active, issue_code)");
    }
}
