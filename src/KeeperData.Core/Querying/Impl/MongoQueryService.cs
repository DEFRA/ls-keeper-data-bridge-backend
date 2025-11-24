using KeeperData.Core.Database;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;

namespace KeeperData.Core.Querying.Impl;

public class MongoQueryService : IMongoQueryService
{
    private readonly IMongoClient _mongoClient;
    private readonly IDatabaseConfig _databaseConfig;
    private readonly IDataSetDefinitions _dataSetDefinitions;
    private readonly ILogger<MongoQueryService> _logger;
    private readonly HashSet<string> _validCollectionNames;
    private readonly DocumentProjector _documentProjector;

    private const int MaxPageSize = 1000;
    private const int DefaultPageSize = 100;

    public MongoQueryService(
        IMongoClient mongoClient,
        IOptions<IDatabaseConfig> databaseConfig,
        IDataSetDefinitions dataSetDefinitions,
        ILogger<MongoQueryService> logger)
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
        string collectionName,
        string? filter = null,
        string? orderBy = null,
        string? select = null,
        int? skip = null,
        int? top = null,
        bool count = true,
        CancellationToken cancellationToken = default)
    {
        ValidateCollectionName(collectionName);

        var sanitizedTop = SanitizeTopParameter(top);
        var sanitizedSkip = skip ?? 0;

        _logger.LogInformation(
            "Executing query on collection '{CollectionName}' with filter='{Filter}', orderBy='{OrderBy}', select='{Select}', skip={Skip}, top={Top}",
            collectionName, filter ?? "none", orderBy ?? "none", select ?? "none", sanitizedSkip, sanitizedTop);

        var collection = GetCollection(collectionName);

        var filterDefinition = BuildFilterDefinition(filter);
        var sortDefinition = BuildSortDefinition(orderBy);
        var fieldsToSelect = ParseSelectExpression(select);

        var documents = await ExecuteQueryAsync(
            collection,
            filterDefinition,
            sortDefinition,
            sanitizedSkip,
            sanitizedTop,
            cancellationToken);

        var totalCount = count
            ? await GetTotalCountAsync(collection, filterDefinition, cancellationToken)
            : (long?)null;

        var data = _documentProjector.ProjectDocuments(documents, fieldsToSelect);

        _logger.LogInformation(
            "Query executed successfully on collection '{CollectionName}'. Returned {Count} records, TotalCount={TotalCount}",
            collectionName, data.Count, totalCount);

        return new QueryResult
        {
            CollectionName = collectionName,
            Data = data,
            Count = data.Count,
            TotalCount = totalCount,
            Skip = sanitizedSkip,
            Top = sanitizedTop,
            Filter = filter,
            OrderBy = orderBy,
            Select = select,
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

    private int SanitizeTopParameter(int? top)
    {
        if (top == null)
        {
            return DefaultPageSize;
        }

        if (top.Value <= 0)
        {
            throw new ArgumentException($"Top parameter must be greater than 0", nameof(top));
        }

        if (top.Value > MaxPageSize)
        {
            _logger.LogWarning(
                "Requested page size {RequestedSize} exceeds maximum {MaxSize}. Using maximum.",
                top.Value, MaxPageSize);
            return MaxPageSize;
        }

        return top.Value;
    }

    private IMongoCollection<BsonDocument> GetCollection(string collectionName)
    {
        var database = _mongoClient.GetDatabase(_databaseConfig.DatabaseName);
        return database.GetCollection<BsonDocument>(collectionName);
    }

    private FilterDefinition<BsonDocument> BuildFilterDefinition(string? filter)
    {
        if (string.IsNullOrWhiteSpace(filter))
        {
            return Builders<BsonDocument>.Filter.Empty;
        }

        try
        {
            // Parse OData-style filter using Microsoft.OData.Core and convert to MongoDB filter
            var translator = new ODataToMongoFilterTranslator();
            return translator.Parse(filter);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to parse filter: {Filter}", filter);
            throw new ArgumentException($"Invalid filter expression: {ex.Message}", nameof(filter), ex);
        }
    }

    private SortDefinition<BsonDocument>? BuildSortDefinition(string? orderBy)
    {
        if (string.IsNullOrWhiteSpace(orderBy))
        {
            return null;
        }

        try
        {
            var parser = new ODataOrderByParser();
            return parser.Parse(orderBy);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to parse orderBy: {OrderBy}", orderBy);
            throw new ArgumentException($"Invalid orderBy expression: {ex.Message}", nameof(orderBy), ex);
        }
    }

    private IReadOnlyList<string>? ParseSelectExpression(string? select)
    {
        if (string.IsNullOrWhiteSpace(select))
        {
            return null;
        }

        try
        {
            var parser = new ODataSelectParser();
            return parser.Parse(select);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to parse select: {Select}", select);
            throw new ArgumentException($"Invalid select expression: {ex.Message}", nameof(select), ex);
        }
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

    private IReadOnlyList<Dictionary<string, object?>> ConvertDocumentsToDictionaries(List<BsonDocument> documents)
    {
        var result = new List<Dictionary<string, object?>>(documents.Count);

        foreach (var doc in documents)
        {
            var dictionary = new Dictionary<string, object?>();

            foreach (var element in doc.Elements)
            {
                dictionary[element.Name] = ConvertBsonValue(element.Value);
            }

            result.Add(dictionary);
        }

        return result;
    }

    private object? ConvertBsonValue(BsonValue value)
    {
        return value.BsonType switch
        {
            BsonType.Null => null,
            BsonType.String => value.AsString,
            BsonType.Int32 => value.AsInt32,
            BsonType.Int64 => value.AsInt64,
            BsonType.Double => value.AsDouble,
            BsonType.Decimal128 => MongoDB.Bson.Decimal128.ToDecimal(value.AsDecimal128),
            BsonType.Boolean => value.AsBoolean,
            BsonType.DateTime => value.ToUniversalTime(),
            BsonType.ObjectId => value.AsObjectId.ToString(),
            BsonType.Array => value.AsBsonArray.Select(ConvertBsonValue).ToList(),
            BsonType.Document => ConvertBsonDocumentToDictionary(value.AsBsonDocument),
            _ => value.ToString()
        };
    }

    private Dictionary<string, object?> ConvertBsonDocumentToDictionary(BsonDocument document)
    {
        var dictionary = new Dictionary<string, object?>();
        foreach (var element in document.Elements)
        {
            dictionary[element.Name] = ConvertBsonValue(element.Value);
        }
        return dictionary;
    }
}