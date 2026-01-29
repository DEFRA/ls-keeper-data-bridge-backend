using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Database;
using KeeperData.Core.Reports.Abstract;
using KeeperData.Core.Reports.Domain;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;

namespace KeeperData.Core.Reports.Impl;

/// <summary>
/// MongoDB implementation of the cleanse report repository.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "MongoDB repository - covered by integration tests.")]
public class CleanseReportRepository : ICleanseReportRepository
{
    private const string CollectionName = "cleanse_report";
    private readonly IMongoCollection<BsonDocument> _collection;

    public CleanseReportRepository(IMongoClient mongoClient, IOptions<IDatabaseConfig> databaseConfig)
    {
        var database = mongoClient.GetDatabase(databaseConfig.Value.DatabaseName);
        _collection = database.GetCollection<BsonDocument>(CollectionName);
    }

    public async Task<CleanseReportItem?> GetByIdAsync(string id, CancellationToken ct = default)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("_id", id);
        var document = await _collection.Find(filter).FirstOrDefaultAsync(ct);
        return document is null ? null : MapToEntity(document);
    }

    public async Task UpsertAsync(CleanseReportItem item, CancellationToken ct = default)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("_id", item.Id);
        var document = MapToDocument(item);
        var options = new ReplaceOptions { IsUpsert = true };
        await _collection.ReplaceOneAsync(filter, document, options, ct);
    }

    public async Task ActivateAsync(string id, DateTime timestamp, CancellationToken ct = default)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("_id", id);
        var update = Builders<BsonDocument>.Update
            .Set("is_active", true)
            .Set("last_updated_at", timestamp);
        await _collection.UpdateOneAsync(filter, update, cancellationToken: ct);
    }

    public async Task DeactivateAsync(string id, DateTime timestamp, CancellationToken ct = default)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("_id", id);
        var update = Builders<BsonDocument>.Update
            .Set("is_active", false)
            .Set("last_updated_at", timestamp);
        await _collection.UpdateOneAsync(filter, update, cancellationToken: ct);
    }

    public async Task<IReadOnlyList<CleanseReportItem>> GetActiveIssuesAsync(int skip, int top, CancellationToken ct = default)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("is_active", true);
        var documents = await _collection
            .Find(filter)
            .Skip(skip)
            .Limit(top)
            .ToListAsync(ct);
        return documents.Select(MapToEntity).ToList();
    }

    public async IAsyncEnumerable<CleanseReportItem> StreamActiveIssuesAsync(
        int batchSize = 1000,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("is_active", true);
        var options = new FindOptions<BsonDocument>
        {
            BatchSize = batchSize
        };

        using var cursor = await _collection.FindAsync(filter, options, ct);

        while (await cursor.MoveNextAsync(ct))
        {
            foreach (var document in cursor.Current)
            {
                ct.ThrowIfCancellationRequested();
                yield return MapToEntity(document);
            }
        }
    }

    public async Task<long> GetActiveIssuesCountAsync(CancellationToken ct = default)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("is_active", true);
        return await _collection.CountDocumentsAsync(filter, cancellationToken: ct);
    }

    public async Task<long> DeleteAllAsync(CancellationToken ct = default)
    {
        var result = await _collection.DeleteManyAsync(Builders<BsonDocument>.Filter.Empty, ct);
        return result.DeletedCount;
    }

    private static BsonDocument MapToDocument(CleanseReportItem item) => new()
    {
        { "_id", item.Id },
        { "code", item.Code },
        { "cts_lid_full_identifier", item.CtsLidFullIdentifier },
        { "cph", item.Cph },
        { "created_at", item.CreatedAtUtc },
        { "last_updated_at", item.LastUpdatedAtUtc },
        { "is_active", item.IsActive }
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
