using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Database;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;

namespace KeeperData.Core.Querying.Impl;

/// <summary>
/// Service for querying MongoDB collections using abstracted .NET types.
/// This service has no dependency on OData or MongoDB-specific types in its public API.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB query service - covered by integration tests.")]
public class QueryService : IQueryService
{
    private readonly IMongoClient _mongoClient;
    private readonly IDatabaseConfig _databaseConfig;
    private readonly IDataSetDefinitions _dataSetDefinitions;
    private readonly ILogger<QueryService> _logger;
    private readonly HashSet<string> _validCollectionNames;
    private readonly DocumentProjector _documentProjector;

    private const int MaxPageSize = 1000;
    private const int DefaultPageSize = 100;

    public QueryService(
        IMongoClient mongoClient,
        IOptions<IDatabaseConfig> databaseConfig,
        IDataSetDefinitions dataSetDefinitions,
        ILogger<QueryService> logger)
    {
        _mongoClient = mongoClient ?? throw new ArgumentNullException(nameof(mongoClient));
        _databaseConfig = databaseConfig?.Value ?? throw new ArgumentNullException(nameof(databaseConfig));
        _dataSetDefinitions = dataSetDefinitions ?? throw new ArgumentNullException(nameof(dataSetDefinitions));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        _validCollectionNames = _dataSetDefinitions.All
            .Select(d => d.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        _documentProjector = new DocumentProjector();
    }

    public async Task<QueryResult> QueryAsync(
        QueryParameters parameters,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(parameters);

        ValidateCollectionName(parameters.CollectionName);

        var sanitizedTop = SanitizeTopParameter(parameters.Top);
        var sanitizedSkip = Math.Max(0, parameters.Skip);

        _logger.LogInformation(
            "Executing query on collection '{CollectionName}' with skip={Skip}, top={Top}",
            parameters.CollectionName, sanitizedSkip, sanitizedTop);

        var collection = GetCollection(parameters.CollectionName);

        // Convert custom filter/sort expressions to MongoDB types
        var filterDefinition = FilterExpressionConverter.ToMongoFilter(parameters.Filter);
        var sortDefinition = SortExpressionConverter.ToMongoSort(parameters.Sort);

        var documents = await ExecuteQueryAsync(
            collection,
            filterDefinition,
            sortDefinition,
            sanitizedSkip,
            sanitizedTop,
            cancellationToken);

        var totalCount = parameters.IncludeCount
            ? await GetTotalCountAsync(collection, filterDefinition, cancellationToken)
            : (long?)null;

        var data = _documentProjector.ProjectDocuments(documents, parameters.FieldsToSelect);

        _logger.LogInformation(
            "Query executed successfully on collection '{CollectionName}'. Returned {Count} records, TotalCount={TotalCount}",
            parameters.CollectionName, data.Count, totalCount);

        return new QueryResult
        {
            CollectionName = parameters.CollectionName,
            Data = data,
            Count = data.Count,
            TotalCount = totalCount,
            Skip = sanitizedSkip,
            Top = sanitizedTop,
            Filter = null,
            OrderBy = null,
            Select = null,
            ExecutedAtUtc = DateTime.UtcNow
        };
    }

    private void ValidateCollectionName(string collectionName)
    {
        if (string.IsNullOrWhiteSpace(collectionName))
        {
            throw new ArgumentException("Collection name cannot be null or empty", nameof(collectionName));
        }

        if (!_validCollectionNames.Contains(collectionName))
        {
            var validNames = string.Join(", ", _validCollectionNames);
            throw new ArgumentException(
                $"Collection '{collectionName}' is not defined in DataSetDefinitions. Valid collections: {validNames}",
                nameof(collectionName));
        }
    }

    private int SanitizeTopParameter(int top)
    {
        if (top <= 0)
        {
            return DefaultPageSize;
        }

        if (top > MaxPageSize)
        {
            _logger.LogWarning(
                "Requested page size {RequestedSize} exceeds maximum {MaxSize}. Using maximum.",
                top, MaxPageSize);
            return MaxPageSize;
        }

        return top;
    }

    private IMongoCollection<BsonDocument> GetCollection(string collectionName)
    {
        var database = _mongoClient.GetDatabase(_databaseConfig.DatabaseName);
        return database.GetCollection<BsonDocument>(collectionName);
    }

    private async Task<List<BsonDocument>> ExecuteQueryAsync(
        IMongoCollection<BsonDocument> collection,
        FilterDefinition<BsonDocument> filter,
        SortDefinition<BsonDocument>? sort,
        int skip,
        int top,
        CancellationToken cancellationToken)
    {
        var findOptions = new FindOptions<BsonDocument>
        {
            Skip = skip,
            Limit = top,
            Sort = sort
        };

        var cursor = await collection.FindAsync(filter, findOptions, cancellationToken);
        return await cursor.ToListAsync(cancellationToken);
    }

    private async Task<long> GetTotalCountAsync(
        IMongoCollection<BsonDocument> collection,
        FilterDefinition<BsonDocument> filter,
        CancellationToken cancellationToken)
    {
        return await collection.CountDocumentsAsync(filter, cancellationToken: cancellationToken);
    }
}
