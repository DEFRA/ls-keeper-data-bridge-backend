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
            await CreateCphIndexAsync(cancellationToken);
            await CreateCtsLidFullIdentifierIndexAsync(cancellationToken);
            await CreateRuleCodeIndexAsync(cancellationToken);
            await CreateErrorCodeIndexAsync(cancellationToken);
            await CreateResolutionStatusIndexAsync(cancellationToken);
            await CreateAssignedToIndexAsync(cancellationToken);
            await CreateTimestampIndexesAsync(cancellationToken);
            await CreateIgnoredIndexAsync(cancellationToken);

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

    /// <summary>
    /// Single-field index on CPH for filter and sort queries.
    /// Query pattern: WHERE cph LIKE '%value%' or ORDER BY cph
    /// </summary>
    private async Task CreateCphIndexAsync(CancellationToken cancellationToken)
    {
        var indexKeys = Builders<IssueDocument>.IndexKeys
            .Ascending(d => d.Cph);

        var indexModel = new CreateIndexModel<IssueDocument>(
            indexKeys,
            new CreateIndexOptions
            {
                Name = "idx_issues_cph",
                Background = true
            });

        await _collection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created index on (cph)");
    }

    /// <summary>
    /// Single-field index on CTS LID full identifier for filter and sort queries.
    /// Query pattern: WHERE cts_lid_full_identifier LIKE '%value%' or ORDER BY cts_lid_full_identifier
    /// </summary>
    private async Task CreateCtsLidFullIdentifierIndexAsync(CancellationToken cancellationToken)
    {
        var indexKeys = Builders<IssueDocument>.IndexKeys
            .Ascending(d => d.CtsLidFullIdentifier);

        var indexModel = new CreateIndexModel<IssueDocument>(
            indexKeys,
            new CreateIndexOptions
            {
                Name = "idx_issues_cts_lid_full_identifier",
                Background = true
            });

        await _collection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created index on (cts_lid_full_identifier)");
    }

    /// <summary>
    /// Single-field index on rule_code for filter and sort queries.
    /// Query pattern: WHERE rule_code = X or ORDER BY rule_code
    /// </summary>
    private async Task CreateRuleCodeIndexAsync(CancellationToken cancellationToken)
    {
        var indexKeys = Builders<IssueDocument>.IndexKeys
            .Ascending(d => d.RuleCode);

        var indexModel = new CreateIndexModel<IssueDocument>(
            indexKeys,
            new CreateIndexOptions
            {
                Name = "idx_issues_rule_code",
                Background = true
            });

        await _collection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created index on (rule_code)");
    }

    /// <summary>
    /// Single-field index on error_code for filter and sort queries.
    /// Query pattern: WHERE error_code = X or ORDER BY error_code
    /// </summary>
    private async Task CreateErrorCodeIndexAsync(CancellationToken cancellationToken)
    {
        var indexKeys = Builders<IssueDocument>.IndexKeys
            .Ascending(d => d.ErrorCode);

        var indexModel = new CreateIndexModel<IssueDocument>(
            indexKeys,
            new CreateIndexOptions
            {
                Name = "idx_issues_error_code",
                Background = true
            });

        await _collection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created index on (error_code)");
    }

    /// <summary>
    /// Single-field index on resolution_status for filter and sort queries.
    /// Query pattern: WHERE resolution_status = X or ORDER BY resolution_status
    /// </summary>
    private async Task CreateResolutionStatusIndexAsync(CancellationToken cancellationToken)
    {
        var indexKeys = Builders<IssueDocument>.IndexKeys
            .Ascending(d => d.ResolutionStatus);

        var indexModel = new CreateIndexModel<IssueDocument>(
            indexKeys,
            new CreateIndexOptions
            {
                Name = "idx_issues_resolution_status",
                Background = true
            });

        await _collection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created index on (resolution_status)");
    }

    /// <summary>
    /// Single-field index on assigned_to for filter and sort queries.
    /// Query pattern: WHERE assigned_to = X or ORDER BY assigned_to
    /// </summary>
    private async Task CreateAssignedToIndexAsync(CancellationToken cancellationToken)
    {
        var indexKeys = Builders<IssueDocument>.IndexKeys
            .Ascending(d => d.AssignedTo);

        var indexModel = new CreateIndexModel<IssueDocument>(
            indexKeys,
            new CreateIndexOptions
            {
                Name = "idx_issues_assigned_to",
                Background = true
            });

        await _collection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created index on (assigned_to)");
    }

    /// <summary>
    /// Indexes on created_at and last_updated_at for date range filter and sort queries.
    /// Query pattern: WHERE created_at > X / ORDER BY created_at, last_updated_at
    /// </summary>
    private async Task CreateTimestampIndexesAsync(CancellationToken cancellationToken)
    {
        var createdAtKeys = Builders<IssueDocument>.IndexKeys
            .Descending(d => d.CreatedAtUtc);

        var createdAtModel = new CreateIndexModel<IssueDocument>(
            createdAtKeys,
            new CreateIndexOptions
            {
                Name = "idx_issues_created_at",
                Background = true
            });

        await _collection.Indexes.CreateOneAsync(createdAtModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created index on (created_at)");

        var updatedAtKeys = Builders<IssueDocument>.IndexKeys
            .Descending(d => d.LastUpdatedAtUtc);

        var updatedAtModel = new CreateIndexModel<IssueDocument>(
            updatedAtKeys,
            new CreateIndexOptions
            {
                Name = "idx_issues_last_updated_at",
                Background = true
            });

        await _collection.Indexes.CreateOneAsync(updatedAtModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created index on (last_updated_at)");
    }

    /// <summary>
    /// Compound index on is_ignored and is_active for filtering by ignored status.
    /// Query pattern: WHERE is_ignored = true/false AND is_active = true
    /// </summary>
    private async Task CreateIgnoredIndexAsync(CancellationToken cancellationToken)
    {
        var indexKeys = Builders<IssueDocument>.IndexKeys
            .Ascending(d => d.IsIgnored)
            .Ascending(d => d.IsActive);

        var indexModel = new CreateIndexModel<IssueDocument>(
            indexKeys,
            new CreateIndexOptions
            {
                Name = "idx_issues_ignored_active",
                Background = true
            });

        await _collection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
        _logger.LogDebug("Created index on (is_ignored, is_active)");
    }
}
