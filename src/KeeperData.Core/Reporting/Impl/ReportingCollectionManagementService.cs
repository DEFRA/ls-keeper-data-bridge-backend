using KeeperData.Core.Database;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace KeeperData.Core.Reporting.Impl;

/// <summary>
/// Implementation of reporting collection management service for MongoDB operations.
/// Manages collections used for import reporting, file tracking, and record lineage.
/// </summary>
public class ReportingCollectionManagementService : IReportingCollectionManagementService
{
    private readonly IMongoClient _mongoClient;
    private readonly IDatabaseConfig _databaseConfig;
    private readonly ILogger<ReportingCollectionManagementService> _logger;

    // Define the known reporting/lineage collections
    private static readonly HashSet<string> ReportingCollections = new(StringComparer.OrdinalIgnoreCase)
    {
        "import_reports",
        "import_files",
        "record_lineage",
        "record_lineage_events"
    };

    public ReportingCollectionManagementService(
        IMongoClient mongoClient,
        IOptions<IDatabaseConfig> databaseConfig,
        ILogger<ReportingCollectionManagementService> logger)
    {
        _mongoClient = mongoClient ?? throw new ArgumentNullException(nameof(mongoClient));
        _databaseConfig = databaseConfig?.Value ?? throw new ArgumentNullException(nameof(databaseConfig));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<DeleteReportingCollectionResult> DeleteReportingCollectionAsync(
        string collectionName, 
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Attempting to delete reporting collection: {CollectionName}", collectionName);

        try
        {
            // Validate that the collection is a known reporting collection
            if (!ReportingCollections.Contains(collectionName))
            {
                var validCollections = string.Join(", ", ReportingCollections);
                var message = $"Collection '{collectionName}' is not a reporting collection. Valid reporting collections: {validCollections}";
                _logger.LogWarning("{Message}", message);
                return new DeleteReportingCollectionResult
                {
                    CollectionName = collectionName,
                    Success = false,
                    Message = message,
                    Error = new ArgumentException(message),
                    OperatedAtUtc = DateTime.UtcNow
                };
            }

            // Normalize the collection name to match the exact casing in MongoDB
            // MongoDB collection names are case-sensitive
            var normalizedCollectionName = ReportingCollections.First(c => 
                c.Equals(collectionName, StringComparison.OrdinalIgnoreCase));

            var database = _mongoClient.GetDatabase(_databaseConfig.DatabaseName);
            await database.DropCollectionAsync(normalizedCollectionName, cancellationToken);

            _logger.LogInformation("Successfully deleted reporting collection: {CollectionName}", normalizedCollectionName);

            return new DeleteReportingCollectionResult
            {
                CollectionName = normalizedCollectionName,
                Success = true,
                Message = $"Reporting collection '{normalizedCollectionName}' deleted successfully.",
                Error = null,
                OperatedAtUtc = DateTime.UtcNow
            };
        }
        catch (OperationCanceledException ex)
        {
            var message = $"Delete reporting collection operation was cancelled for collection: {collectionName}";
            _logger.LogWarning(ex, "{Message}", message);
            return new DeleteReportingCollectionResult
            {
                CollectionName = collectionName,
                Success = false,
                Message = message,
                Error = ex,
                OperatedAtUtc = DateTime.UtcNow
            };
        }
        catch (Exception ex)
        {
            var message = $"Failed to delete reporting collection '{collectionName}': {ex.Message}";
            _logger.LogError(ex, "{Message}", message);
            return new DeleteReportingCollectionResult
            {
                CollectionName = collectionName,
                Success = false,
                Message = message,
                Error = ex,
                OperatedAtUtc = DateTime.UtcNow
            };
        }
    }

    public async Task<DeleteAllReportingCollectionsResult> DeleteAllReportingCollectionsAsync(
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Attempting to delete all reporting collections");

        try
        {
            var database = _mongoClient.GetDatabase(_databaseConfig.DatabaseName);
            var deletedCollections = new List<string>();

            // Get list of existing collections first
            var existingCollections = await GetExistingCollectionsAsync(database, cancellationToken);

            foreach (var collectionName in ReportingCollections)
            {
                // Only attempt to delete if the collection exists
                if (!existingCollections.Contains(collectionName))
                {
                    _logger.LogWarning("Reporting collection {CollectionName} does not exist, skipping", collectionName);
                    continue;
                }

                try
                {
                    await database.DropCollectionAsync(collectionName, cancellationToken);
                    deletedCollections.Add(collectionName);
                    _logger.LogInformation("Deleted reporting collection: {CollectionName}", collectionName);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to delete reporting collection {CollectionName}", collectionName);
                    // Continue with next collection
                }
            }

            _logger.LogInformation("Successfully deleted {Count} reporting collection(s)", deletedCollections.Count);

            return new DeleteAllReportingCollectionsResult
            {
                DeletedCollections = deletedCollections,
                TotalCount = deletedCollections.Count,
                Success = true,
                Message = $"{deletedCollections.Count} reporting collection(s) deleted successfully.",
                Error = null,
                OperatedAtUtc = DateTime.UtcNow
            };
        }
        catch (OperationCanceledException ex)
        {
            var message = "Delete all reporting collections operation was cancelled";
            _logger.LogWarning(ex, "{Message}", message);
            return new DeleteAllReportingCollectionsResult
            {
                DeletedCollections = new List<string>(),
                TotalCount = 0,
                Success = false,
                Message = message,
                Error = ex,
                OperatedAtUtc = DateTime.UtcNow
            };
        }
        catch (Exception ex)
        {
            var message = $"Failed to delete reporting collections: {ex.Message}";
            _logger.LogError(ex, "{Message}", message);
            return new DeleteAllReportingCollectionsResult
            {
                DeletedCollections = new List<string>(),
                TotalCount = 0,
                Success = false,
                Message = message,
                Error = ex,
                OperatedAtUtc = DateTime.UtcNow
            };
        }
    }

    /// <summary>
    /// Helper method to get list of existing collections in the database
    /// </summary>
    private async Task<HashSet<string>> GetExistingCollectionsAsync(
        IMongoDatabase database,
        CancellationToken cancellationToken)
    {
        var collections = await database.ListCollectionNamesAsync(cancellationToken: cancellationToken);
        var collectionList = await collections.ToListAsync(cancellationToken);
        return new HashSet<string>(collectionList, StringComparer.OrdinalIgnoreCase);
    }
}
