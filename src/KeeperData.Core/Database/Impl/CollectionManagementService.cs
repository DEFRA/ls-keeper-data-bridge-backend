using KeeperData.Core.ETL.Abstract;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace KeeperData.Core.Database.Impl;

/// <summary>
/// Implementation of collection management service for MongoDB operations.
/// </summary>
public class CollectionManagementService : ICollectionManagementService
{
    private readonly IMongoClient _mongoClient;
    private readonly IDatabaseConfig _databaseConfig;
    private readonly IDataSetDefinitions _dataSetDefinitions;
    private readonly ILogger<CollectionManagementService> _logger;

    public CollectionManagementService(
        IMongoClient mongoClient,
        IOptions<IDatabaseConfig> databaseConfig,
        IDataSetDefinitions dataSetDefinitions,
        ILogger<CollectionManagementService> logger)
    {
        _mongoClient = mongoClient ?? throw new ArgumentNullException(nameof(mongoClient));
        _databaseConfig = databaseConfig?.Value ?? throw new ArgumentNullException(nameof(databaseConfig));
        _dataSetDefinitions = dataSetDefinitions ?? throw new ArgumentNullException(nameof(dataSetDefinitions));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<DeleteCollectionResult> DeleteCollectionAsync(string collectionName, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Attempting to delete collection: {CollectionName}", collectionName);

        try
        {
            // Validate that the collection is defined in DataSetDefinitions
            var collectionDefined = _dataSetDefinitions.All.Any(d => d.Name.Equals(collectionName, StringComparison.OrdinalIgnoreCase));

            if (!collectionDefined)
            {
                var message = $"Collection '{collectionName}' is not defined in DataSetDefinitions.";
                _logger.LogWarning("{Message}", message);
                return new DeleteCollectionResult
                {
                    CollectionName = collectionName,
                    Success = false,
                    Message = message,
                    Error = new ArgumentException(message),
                    OperatedAtUtc = DateTime.UtcNow
                };
            }

            var database = _mongoClient.GetDatabase(_databaseConfig.DatabaseName);
            await database.DropCollectionAsync(collectionName, cancellationToken);

            _logger.LogInformation("Successfully deleted collection: {CollectionName}", collectionName);

            return new DeleteCollectionResult
            {
                CollectionName = collectionName,
                Success = true,
                Message = $"Collection '{collectionName}' deleted successfully.",
                Error = null,
                OperatedAtUtc = DateTime.UtcNow
            };
        }
        catch (OperationCanceledException ex)
        {
            var message = $"Delete collection operation was cancelled for collection: {collectionName}";
            _logger.LogWarning(ex, "{Message}", message);
            return new DeleteCollectionResult
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
            var message = $"Failed to delete collection '{collectionName}': {ex.Message}";
            _logger.LogError(ex, "{Message}", message);
            return new DeleteCollectionResult
            {
                CollectionName = collectionName,
                Success = false,
                Message = message,
                Error = ex,
                OperatedAtUtc = DateTime.UtcNow
            };
        }
    }

    public async Task<DeleteAllCollectionsResult> DeleteAllCollectionsAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Attempting to delete all collections");

        try
        {
            var database = _mongoClient.GetDatabase(_databaseConfig.DatabaseName);
            var collectionsToDelete = _dataSetDefinitions.All.Select(d => d.Name).ToList();
            var deletedCollections = new List<string>();

            foreach (var collectionName in collectionsToDelete)
            {
                try
                {
                    await database.DropCollectionAsync(collectionName, cancellationToken);
                    deletedCollections.Add(collectionName);
                    _logger.LogInformation("Deleted collection: {CollectionName}", collectionName);
                }
                catch (MongoCommandException ex) when (ex.Code == 26) // Namespace not found (collection doesn't exist)
                {
                    _logger.LogWarning("Collection {CollectionName} does not exist, skipping", collectionName);
                    // Continue with next collection
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to delete collection {CollectionName}", collectionName);
                    // Continue with next collection
                }
            }

            _logger.LogInformation("Successfully deleted {Count} collection(s)", deletedCollections.Count);

            return new DeleteAllCollectionsResult
            {
                DeletedCollections = deletedCollections,
                TotalCount = deletedCollections.Count,
                Success = true,
                Message = $"{deletedCollections.Count} collection(s) deleted successfully.",
                Error = null,
                OperatedAtUtc = DateTime.UtcNow
            };
        }
        catch (OperationCanceledException ex)
        {
            var message = "Delete all collections operation was cancelled";
            _logger.LogWarning(ex, "{Message}", message);
            return new DeleteAllCollectionsResult
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
            var message = $"Failed to delete collections: {ex.Message}";
            _logger.LogError(ex, "{Message}", message);
            return new DeleteAllCollectionsResult
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
}