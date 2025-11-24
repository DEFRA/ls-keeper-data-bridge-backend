using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Polly;

namespace KeeperData.Core.Database.Resilience;

/// <summary>
/// Wrapper for MongoDB operations with built-in resilience policies.
/// Encapsulates retry logic, circuit breaker, and timeout handling.
/// </summary>
public class ResilientMongoOperations
{
    private readonly ILogger<ResilientMongoOperations> _logger;
    private readonly Configuration.MongoResilienceConfig _config;
    private readonly ResiliencePipeline _bulkWritePipeline;
    private readonly ResiliencePipeline<List<BsonDocument>> _findPipeline;
    private readonly ResiliencePipeline _collectionManagementPipeline;

    public ResilientMongoOperations(
        IOptions<Configuration.MongoResilienceConfig> config,
        ILogger<ResilientMongoOperations> logger)
    {
        _logger = logger;
        _config = config.Value;

        _bulkWritePipeline = MongoResiliencePipelineFactory.CreateForVoid(
            _config, _logger, "BulkWrite");

        _findPipeline = MongoResiliencePipelineFactory.Create<List<BsonDocument>>(
            _config, _logger, "Find");

        _collectionManagementPipeline = MongoResiliencePipelineFactory.CreateForVoid(
            _config, _logger, "CollectionManagement");
    }

    /// <summary>
    /// Executes a MongoDB bulk write operation with resilience.
    /// </summary>
    public async Task BulkWriteAsync(
        IMongoCollection<BsonDocument> collection,
        List<WriteModel<BsonDocument>> operations,
        BulkWriteOptions? options = null,
        CancellationToken ct = default)
    {
        if (operations.Count == 0)
        {
            return;
        }

        await _bulkWritePipeline.ExecuteAsync(
            async cancellationToken =>
            {
                await collection.BulkWriteAsync(
                    operations,
                    options ?? new BulkWriteOptions { IsOrdered = false },
                    cancellationToken);
            },
            ct);
    }

    /// <summary>
    /// Executes a MongoDB find operation with resilience and returns results as a list.
    /// </summary>
    public async Task<Dictionary<BsonValue, BsonDocument>> FindAndMapAsync(
        IMongoCollection<BsonDocument> collection,
        FilterDefinition<BsonDocument> filter,
        CancellationToken ct = default)
    {
        var documents = await _findPipeline.ExecuteAsync(
            async cancellationToken =>
            {
                var cursor = await collection.FindAsync(filter, cancellationToken: cancellationToken);
                return await cursor.ToListAsync(cancellationToken);
            },
            ct);

        return documents.ToDictionary(doc => doc["_id"], doc => doc);
    }

    /// <summary>
    /// Executes a MongoDB collection creation operation with resilience.
    /// </summary>
    public async Task CreateCollectionAsync(
        IMongoDatabase database,
        string collectionName,
        CancellationToken ct = default)
    {
        await _collectionManagementPipeline.ExecuteAsync(
            async cancellationToken =>
            {
                await database.CreateCollectionAsync(collectionName, cancellationToken: cancellationToken);
            },
            ct);
    }

    /// <summary>
    /// Executes a MongoDB list collections operation with resilience.
    /// </summary>
    public async Task<List<string>> ListCollectionNamesAsync(
        IMongoDatabase database,
        CancellationToken ct = default)
    {
        return await MongoResiliencePipelineFactory
            .Create<List<string>>(_config, _logger, "ListCollections")
            .ExecuteAsync(
                async cancellationToken =>
                {
                    var cursor = await database.ListCollectionNamesAsync(cancellationToken: cancellationToken);
                    return await cursor.ToListAsync(cancellationToken);
                },
                ct);
    }

    /// <summary>
    /// Executes a MongoDB index creation operation with resilience.
    /// </summary>
    public async Task CreateIndexAsync(
        IMongoCollection<BsonDocument> collection,
        CreateIndexModel<BsonDocument> indexModel,
        CancellationToken ct = default)
    {
        await _collectionManagementPipeline.ExecuteAsync(
            async cancellationToken =>
            {
                await collection.Indexes.CreateOneAsync(indexModel, cancellationToken: cancellationToken);
            },
            ct);
    }

    /// <summary>
    /// Executes a MongoDB list indexes operation with resilience.
    /// </summary>
    public async Task<List<BsonDocument>> ListIndexesAsync(
        IMongoCollection<BsonDocument> collection,
        CancellationToken ct = default)
    {
        return await MongoResiliencePipelineFactory
            .Create<List<BsonDocument>>(_config, _logger, "ListIndexes")
            .ExecuteAsync(
                async cancellationToken =>
                {
                    var cursor = await collection.Indexes.ListAsync(cancellationToken);
                    return await cursor.ToListAsync(cancellationToken);
                },
                ct);
    }
}