using System.Runtime.CompilerServices;
using KeeperData.Core.Reports.Internal.Collections;
using KeeperData.Core.Reports.Internal.Documents;
using KeeperData.Core.Reports.Internal.Mappers;
using KeeperData.Core.Reports.Issues.Query.Abstract;
using KeeperData.Core.Reports.Issues.Query.Dtos;
using MongoDB.Driver;

namespace KeeperData.Core.Reports.Issues.Query;

public class IssueQueries(IssueCollection issueCollection, IssueHistoryCollection historyCollection) : IIssueQueries
{
    private readonly IMongoCollection<IssueDocument> _collection = issueCollection.Collection;
    private readonly IMongoCollection<IssueHistoryEntryDocument> _historyCollection = historyCollection.Collection;

    public async Task<long> GetActiveIssuesCountAsync(CancellationToken ct = default)
    {
        var filter = Builders<IssueDocument>.Filter.Eq(d => d.IsActive, true);
        return await _collection.CountDocumentsAsync(filter, cancellationToken: ct);
    }

    public async Task<IssueDto?> GetByIdAsync(string id, CancellationToken ct = default)
    {
        var filter = Builders<IssueDocument>.Filter.Eq(d => d.Id, id);
        var document = await _collection.Find(filter).FirstOrDefaultAsync(ct);
        return document?.ToDto();
    }

    public async Task<IReadOnlyList<IssueDto>> GetActiveIssuesAsync(int skip, int top, CancellationToken ct = default)
    {
        var filter = Builders<IssueDocument>.Filter.Eq(d => d.IsActive, true);
        var documents = await _collection
            .Find(filter)
            .Skip(skip)
            .Limit(top)
            .ToListAsync(ct);
        return documents.Select(d => d.ToDto()).ToList();
    }

    public async IAsyncEnumerable<IssueDto> StreamActiveIssuesAsync(
        int batchSize = 1000,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var filter = Builders<IssueDocument>.Filter.Eq(d => d.IsActive, true);
        var options = new FindOptions<IssueDocument> { BatchSize = batchSize };

        using var cursor = await _collection.FindAsync(filter, options, ct);

        while (await cursor.MoveNextAsync(ct))
        {
            foreach (var document in cursor.Current)
            {
                ct.ThrowIfCancellationRequested();
                yield return document.ToDto();
            }
        }
    }

    public async Task<CleanseIssueQueryResultDto> QueryAsync(CleanseIssueQueryDto query, CancellationToken ct = default)
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

        return new CleanseIssueQueryResultDto
        {
            Items = itemsTask.Result.Select(d => d.ToDto()).ToList(),
            TotalCount = (int)countTask.Result,
            Skip = query.Skip,
            Top = query.Top
        };
    }

    public async Task<int> CountAsync(CleanseIssueQueryDto query, CancellationToken ct = default)
    {
        var filter = BuildFilter(query);
        var count = await _collection.CountDocumentsAsync(filter, cancellationToken: ct);
        return (int)count;
    }

    public async Task<IReadOnlyList<CleanseIssueGroupResultDto>> GroupByIssueCodeAsync(
        CleanseIssueQueryDto? baseFilter = null, int itemsPerGroup = 10, CancellationToken ct = default)
    {
        var matchFilter = baseFilter is not null ? BuildFilter(baseFilter) : Builders<IssueDocument>.Filter.Empty;
        var sortField = baseFilter?.SortBy ?? CleanseIssueSortField.Cph;
        var sortDescending = baseFilter?.SortDescending ?? false;

        var sortDocument = BuildSortDocument(sortField, sortDescending);

        // Aggregation pipeline requires BsonDocument rendering
        var serializerRegistry = MongoDB.Bson.Serialization.BsonSerializer.SerializerRegistry;
        var documentSerializer = serializerRegistry.GetSerializer<IssueDocument>();
        var renderedFilter = matchFilter.Render(documentSerializer, serializerRegistry);

        var pipeline = new[]
        {
            new MongoDB.Bson.BsonDocument("$match", renderedFilter),
            new MongoDB.Bson.BsonDocument("$sort", sortDocument),
            new MongoDB.Bson.BsonDocument("$group", new MongoDB.Bson.BsonDocument
            {
                { "_id", "$issue_code" },
                { "totalCount", new MongoDB.Bson.BsonDocument("$sum", 1) },
                { "items", new MongoDB.Bson.BsonDocument("$push", "$$ROOT") }
            }),
            new MongoDB.Bson.BsonDocument("$project", new MongoDB.Bson.BsonDocument
            {
                { "_id", 0 },
                { "issueCode", "$_id" },
                { "totalCount", 1 },
                { "items", new MongoDB.Bson.BsonDocument("$slice", new MongoDB.Bson.BsonArray { "$items", itemsPerGroup }) }
            }),
            new MongoDB.Bson.BsonDocument("$sort", new MongoDB.Bson.BsonDocument("totalCount", -1))
        };

        var results = await (await _collection.AggregateAsync<MongoDB.Bson.BsonDocument>(pipeline, cancellationToken: ct)).ToListAsync(ct);

        return results.Select(doc => new CleanseIssueGroupResultDto
        {
            IssueCode = doc["issueCode"].AsString,
            TotalCount = doc["totalCount"].AsInt32,
            Items = doc["items"].AsBsonArray
                .Select(i => MongoDB.Bson.Serialization.BsonSerializer.Deserialize<IssueDocument>(i.AsBsonDocument).ToDto())
                .ToList()
        }).ToList();
    }

    public async Task<CleanseIssuesResultDto> ListIssuesAsync(int skip = 0, int top = 50, CancellationToken ct = default)
    {
        var itemsTask = GetActiveIssuesAsync(skip, top, ct);
        var countTask = GetActiveIssuesCountAsync(ct);

        await Task.WhenAll(itemsTask, countTask);

        return new CleanseIssuesResultDto
        {
            Items = itemsTask.Result,
            TotalCount = (int)countTask.Result,
            Skip = skip,
            Top = top
        };
    }

    public async Task<IReadOnlyList<IssueHistoryEntryDto>> GetIssueHistoryAsync(string issueId, int skip = 0, int top = 50, CancellationToken ct = default)
    {
        var filter = Builders<IssueHistoryEntryDocument>.Filter.Eq(d => d.IssueId, issueId);
        var sort = Builders<IssueHistoryEntryDocument>.Sort.Descending(d => d.OccurredAtUtc);

        var documents = await _historyCollection.Find(filter)
            .Sort(sort)
            .Skip(skip)
            .Limit(top)
            .ToListAsync(ct);

        return documents.Select(d => d.ToDto()).ToList();
    }

    public async Task<long> GetIssueHistoryCountAsync(string issueId, CancellationToken ct = default)
    {
        var filter = Builders<IssueHistoryEntryDocument>.Filter.Eq(d => d.IssueId, issueId);
        return await _historyCollection.CountDocumentsAsync(filter, cancellationToken: ct);
    }

    #region Helpers

    private static FilterDefinition<IssueDocument> BuildFilter(CleanseIssueQueryDto query)
    {
        var builder = Builders<IssueDocument>.Filter;
        var filters = new List<FilterDefinition<IssueDocument>>();

        if (query.IsActive.HasValue)
            filters.Add(builder.Eq(d => d.IsActive, query.IsActive.Value));

        if (!string.IsNullOrEmpty(query.IssueCode))
            filters.Add(builder.Eq(d => d.IssueCode, query.IssueCode));

        if (!string.IsNullOrEmpty(query.CphContains))
            filters.Add(builder.Regex(d => d.Cph, new MongoDB.Bson.BsonRegularExpression(query.CphContains, "i")));

        if (!string.IsNullOrEmpty(query.CphStartsWith))
            filters.Add(builder.Regex(d => d.Cph, new MongoDB.Bson.BsonRegularExpression($"^{query.CphStartsWith}", "i")));

        if (query.CreatedAfterUtc.HasValue)
            filters.Add(builder.Gt(d => d.CreatedAtUtc, query.CreatedAfterUtc.Value));

        if (query.CreatedBeforeUtc.HasValue)
            filters.Add(builder.Lt(d => d.CreatedAtUtc, query.CreatedBeforeUtc.Value));

        if (query.UpdatedAfterUtc.HasValue)
            filters.Add(builder.Gt(d => d.LastUpdatedAtUtc, query.UpdatedAfterUtc.Value));

        if (query.UpdatedBeforeUtc.HasValue)
            filters.Add(builder.Lt(d => d.LastUpdatedAtUtc, query.UpdatedBeforeUtc.Value));

        if (query.IsIgnored.HasValue)
            filters.Add(builder.Eq(d => d.IsIgnored, query.IsIgnored.Value));

        if (!string.IsNullOrEmpty(query.ResolutionStatus))
            filters.Add(builder.Eq(d => d.ResolutionStatus, query.ResolutionStatus));

        if (!string.IsNullOrEmpty(query.AssignedTo))
            filters.Add(builder.Eq(d => d.AssignedTo, query.AssignedTo));

        if (query.IsUnassigned == true)
            filters.Add(builder.Eq(d => d.AssignedTo, null));

        return filters.Count > 0 ? builder.And(filters) : builder.Empty;
    }

    private static SortDefinition<IssueDocument> BuildSort(CleanseIssueQueryDto query)
    {
        return query.SortBy switch
        {
            CleanseIssueSortField.Cph => query.SortDescending
                ? Builders<IssueDocument>.Sort.Descending(d => d.Cph)
                : Builders<IssueDocument>.Sort.Ascending(d => d.Cph),
            CleanseIssueSortField.IssueCode => query.SortDescending
                ? Builders<IssueDocument>.Sort.Descending(d => d.IssueCode)
                : Builders<IssueDocument>.Sort.Ascending(d => d.IssueCode),
            CleanseIssueSortField.CreatedAtUtc => query.SortDescending
                ? Builders<IssueDocument>.Sort.Descending(d => d.CreatedAtUtc)
                : Builders<IssueDocument>.Sort.Ascending(d => d.CreatedAtUtc),
            _ => query.SortDescending
                ? Builders<IssueDocument>.Sort.Descending(d => d.LastUpdatedAtUtc)
                : Builders<IssueDocument>.Sort.Ascending(d => d.LastUpdatedAtUtc)
        };
    }

    private static MongoDB.Bson.BsonDocument BuildSortDocument(CleanseIssueSortField field, bool descending)
    {
        var fieldName = field switch
        {
            CleanseIssueSortField.Cph => "cph",
            CleanseIssueSortField.IssueCode => "issue_code",
            CleanseIssueSortField.CreatedAtUtc => "created_at",
            _ => "last_updated_at"
        };
        return new MongoDB.Bson.BsonDocument(fieldName, descending ? -1 : 1);
    }

    #endregion
}