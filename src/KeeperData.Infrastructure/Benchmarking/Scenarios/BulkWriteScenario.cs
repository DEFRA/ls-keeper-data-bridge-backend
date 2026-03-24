using MongoDB.Bson;
using MongoDB.Driver;

namespace KeeperData.Infrastructure.Benchmarking.Scenarios;

/// <summary>
/// Bulk-write scenario: batches of upserts against the benchmark write collection.
/// Uses deterministic IDs so each batch is repeatable.
/// </summary>
public sealed class BulkWriteScenario : ScenarioBase
{
    private readonly IMongoCollection<BsonDocument> _collection;
    private const int BatchSize = 50;

    public BulkWriteScenario(IMongoCollection<BsonDocument> collection)
    {
        _collection = collection;
    }

    public override string Name => "BulkWrite";

    protected override async Task<bool> ExecuteOperationAsync(int iteration, CancellationToken ct)
    {
        var models = new List<WriteModel<BsonDocument>>(BatchSize);

        for (var i = 0; i < BatchSize; i++)
        {
            var id = $"bulk-{(iteration * BatchSize + i) % 50_000:D8}";
            var doc = new BsonDocument
            {
                { "_id", id },
                { "iteration", iteration },
                { "index", i },
                { "updatedAt", DateTime.UtcNow },
                { "payload", new string('x', 200) }
            };

            var filter = Builders<BsonDocument>.Filter.Eq("_id", id);
            models.Add(new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true });
        }

        var result = await _collection.BulkWriteAsync(models, new BulkWriteOptions { IsOrdered = false }, ct);
        return result.IsAcknowledged;
    }
}
