using KeeperData.Core.Database;
using KeeperData.Core.Reports.Domain;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;

namespace KeeperData.Core.Reports.Querying;

/// <summary>
/// MongoDB implementation of the cleanse issue query service.
/// </summary>
public sealed class CleanseIssueQueryService : ICleanseIssueQueryService
{
    private const string CollectionName = "cleanse_report";
    private readonly IMongoCollection<BsonDocument> _collection;

    public CleanseIssueQueryService(IMongoClient mongoClient, IOptions<IDatabaseConfig> databaseConfig)
    {
        var database = mongoClient.GetDatabase(databaseConfig.Value.DatabaseName);
        _collection = database.GetCollection<BsonDocument>(CollectionName);
    }

    /// <inheritdoc />
    public async Task<CleanseIssueQueryResult> QueryAsync(CleanseIssueQuery query, CancellationToken ct = default)
    {
        var filter = BuildFilter(query);
        var sort = BuildSort(query);

        var countTask = _collection.CountDocumentsAsync(filter, cancellationToken: ct);
        var itemsTask = _collection
            .Find(filter)
            .Sort(sort)
            .Skip(query.Skip)
            .Limit(query.Top)
            .ToListAsync(ct);

        await Task.WhenAll(countTask, itemsTask);

        return new CleanseIssueQueryResult
        {
            Items = itemsTask.Result.Select(MapToEntity).ToList(),
            TotalCount = (int)countTask.Result,
            Skip = query.Skip,
            Top = query.Top
        };
    }

    /// <inheritdoc />
    public async Task<int> CountAsync(CleanseIssueQuery query, CancellationToken ct = default)
    {
        var filter = BuildFilter(query);
        var count = await _collection.CountDocumentsAsync(filter, cancellationToken: ct);
        return (int)count;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<CleanseIssueGroupResult>> GroupByIssueCodeAsync(
        CleanseIssueQuery? baseFilter = null,
        int itemsPerGroup = 10,
        CancellationToken ct = default)
    {
        var matchFilter = baseFilter is not null ? BuildFilter(baseFilter) : FilterDefinition<BsonDocument>.Empty;
        var sortField = baseFilter?.SortBy ?? CleanseIssueSortField.Cph;
        var sortDescending = baseFilter?.SortDescending ?? false;

        var sortDocument = BuildSortDocument(sortField, sortDescending);

        var pipeline = new[]
        {
            new BsonDocument("$match", matchFilter.Render(
                _collection.DocumentSerializer,
                _collection.Settings.SerializerRegistry)),
            new BsonDocument("$sort", sortDocument),
            new BsonDocument("$group", new BsonDocument
            {
                { "_id", "$code" },
                { "totalCount", new BsonDocument("$sum", 1) },
                { "items", new BsonDocument("$push", "$$ROOT") }
            }),
            new BsonDocument("$project", new BsonDocument
            {
                { "_id", 0 },
                { "issueCode", "$_id" },
                { "totalCount", 1 },
                { "items", new BsonDocument("$slice", new BsonArray { "$items", itemsPerGroup }) }
            }),
            new BsonDocument("$sort", new BsonDocument("totalCount", -1))
        };

        var results = await _collection.Aggregate<BsonDocument>(pipeline, cancellationToken: ct).ToListAsync(ct);

        return results.Select(doc => new CleanseIssueGroupResult
        {
            IssueCode = doc["issueCode"].AsString,
            TotalCount = doc["totalCount"].AsInt32,
            Items = doc["items"].AsBsonArray.Select(i => MapToEntity(i.AsBsonDocument)).ToList()
        }).ToList();
    }

    private static FilterDefinition<BsonDocument> BuildFilter(CleanseIssueQuery query)
    {
        var builder = Builders<BsonDocument>.Filter;
        var filters = new List<FilterDefinition<BsonDocument>>();

        if (query.IsActive.HasValue)
        {
            filters.Add(builder.Eq("is_active", query.IsActive.Value));
        }

        if (!string.IsNullOrEmpty(query.IssueCode))
        {
            filters.Add(builder.Eq("code", query.IssueCode));
        }

        if (!string.IsNullOrEmpty(query.CphContains))
        {
            filters.Add(builder.Regex("cph", new BsonRegularExpression(query.CphContains, "i")));
        }

        if (!string.IsNullOrEmpty(query.CphStartsWith))
        {
            filters.Add(builder.Regex("cph", new BsonRegularExpression($"^{query.CphStartsWith}", "i")));
        }

        if (query.CreatedAfterUtc.HasValue)
        {
            filters.Add(builder.Gt("created_at", query.CreatedAfterUtc.Value));
        }

        if (query.CreatedBeforeUtc.HasValue)
        {
            filters.Add(builder.Lt("created_at", query.CreatedBeforeUtc.Value));
        }

        if (query.UpdatedAfterUtc.HasValue)
        {
            filters.Add(builder.Gt("last_updated_at", query.UpdatedAfterUtc.Value));
        }

        if (query.UpdatedBeforeUtc.HasValue)
        {
            filters.Add(builder.Lt("last_updated_at", query.UpdatedBeforeUtc.Value));
        }

        return filters.Count > 0 ? builder.And(filters) : FilterDefinition<BsonDocument>.Empty;
    }

    private static SortDefinition<BsonDocument> BuildSort(CleanseIssueQuery query)
    {
        var builder = Builders<BsonDocument>.Sort;
        var fieldName = GetSortFieldName(query.SortBy);

        return query.SortDescending
            ? builder.Descending(fieldName)
            : builder.Ascending(fieldName);
    }

    private static BsonDocument BuildSortDocument(CleanseIssueSortField field, bool descending)
    {
        var fieldName = GetSortFieldName(field);
        return new BsonDocument(fieldName, descending ? -1 : 1);
    }

    private static string GetSortFieldName(CleanseIssueSortField field) => field switch
    {
        CleanseIssueSortField.Cph => "cph",
        CleanseIssueSortField.IssueCode => "code",
        CleanseIssueSortField.CreatedAtUtc => "created_at",
        CleanseIssueSortField.LastUpdatedAtUtc => "last_updated_at",
        _ => "last_updated_at"
    };

    private static CleanseReportItem MapToEntity(BsonDocument document) => new()
    {
        Id = document["_id"].AsString,
        Code = document["code"].AsString,
        CtsLidFullIdentifier = document["cts_lid_full_identifier"].AsString,
        Cph = document["cph"].AsString,
        CreatedAtUtc = document["created_at"].ToUniversalTime(),
        LastUpdatedAtUtc = document["last_updated_at"].ToUniversalTime(),
        IsActive = document["is_active"].AsBoolean
    };
}
