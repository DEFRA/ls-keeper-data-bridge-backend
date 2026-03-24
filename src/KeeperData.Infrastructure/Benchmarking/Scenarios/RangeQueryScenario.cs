using MongoDB.Bson;
using MongoDB.Driver;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.Benchmarking.Scenarios;

/// <summary>
/// Range query on an indexed <c>createdAt</c> + <c>status</c> compound field,
/// representative of the kind of queries the data-cleanse analysis performs.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Executes MongoDB range queries — covered by performance tests.")]
public sealed class RangeQueryScenario : ScenarioBase
{
    private readonly IMongoCollection<BsonDocument> _collection;
    private readonly DateTime _baseDate;

    public RangeQueryScenario(IMongoCollection<BsonDocument> collection, DateTime baseDate)
    {
        _collection = collection;
        _baseDate = baseDate;
    }

    public override string Name => "RangeQuery";

    protected override async Task<bool> ExecuteOperationAsync(int iteration, CancellationToken ct)
    {
        // Rotate through 10 deterministic date windows and 3 statuses
        var dayOffset = iteration % 10;
        var status = (iteration % 3) switch
        {
            0 => "Active",
            1 => "Pending",
            _ => "Archived"
        };

        var from = _baseDate.AddDays(dayOffset);
        var to = from.AddDays(1);

        var filter = Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Gte("createdAt", from),
            Builders<BsonDocument>.Filter.Lt("createdAt", to),
            Builders<BsonDocument>.Filter.Eq("status", status));

        var count = await _collection.Find(filter).Limit(100).CountDocumentsAsync(ct);
        return count > 0;
    }
}
