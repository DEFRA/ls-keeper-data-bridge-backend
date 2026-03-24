using MongoDB.Bson;
using MongoDB.Driver;

namespace KeeperData.Infrastructure.Benchmarking.Scenarios;

/// <summary>
/// Runs a representative aggregation pipeline: match → group → sort.
/// Uses the compound index on <c>status</c> and <c>category</c>.
/// </summary>
public sealed class AggregationScenario : ScenarioBase
{
    private readonly IMongoCollection<BsonDocument> _collection;

    public AggregationScenario(IMongoCollection<BsonDocument> collection)
    {
        _collection = collection;
    }

    public override string Name => "Aggregation";

    protected override async Task<bool> ExecuteOperationAsync(int iteration, CancellationToken ct)
    {
        var status = (iteration % 3) switch
        {
            0 => "Active",
            1 => "Pending",
            _ => "Archived"
        };

        var pipeline = new[]
        {
            new BsonDocument("$match", new BsonDocument("status", status)),
            new BsonDocument("$group", new BsonDocument
            {
                { "_id", "$category" },
                { "count", new BsonDocument("$sum", 1) },
                { "avgValue", new BsonDocument("$avg", "$numericValue") }
            }),
            new BsonDocument("$sort", new BsonDocument("count", -1)),
            new BsonDocument("$limit", 20)
        };

        var cursor = await _collection.AggregateAsync<BsonDocument>(
            PipelineDefinition<BsonDocument, BsonDocument>.Create(pipeline),
            cancellationToken: ct);

        var results = await cursor.ToListAsync(ct);
        return results.Count >= 0;
    }
}
