using MongoDB.Bson;
using MongoDB.Driver;

namespace KeeperData.Infrastructure.Benchmarking.Scenarios;

/// <summary>
/// Indexed point-lookup by <c>_id</c>. Deterministic ID derived from iteration index.
/// </summary>
public sealed class PointLookupScenario : ScenarioBase
{
    private readonly IMongoCollection<BsonDocument> _collection;
    private readonly int _seedCount;

    public PointLookupScenario(IMongoCollection<BsonDocument> collection, int seedCount)
    {
        _collection = collection;
        _seedCount = seedCount;
    }

    public override string Name => "PointLookup";

    protected override async Task<bool> ExecuteOperationAsync(int iteration, CancellationToken ct)
    {
        var id = $"bench-{iteration % _seedCount:D8}";
        var filter = Builders<BsonDocument>.Filter.Eq("_id", id);
        var doc = await _collection.Find(filter).FirstOrDefaultAsync(ct);
        return doc is not null;
    }
}
