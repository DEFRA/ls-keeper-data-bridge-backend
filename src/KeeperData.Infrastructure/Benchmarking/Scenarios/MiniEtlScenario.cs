using MongoDB.Bson;
using MongoDB.Driver;

namespace KeeperData.Infrastructure.Benchmarking.Scenarios;

/// <summary>
/// Mini-ETL simulation: read a document from the source collection,
/// look up a reference document, transform, then write to the write collection.
/// This mirrors the data-cleanse read→analyse→write pattern.
/// </summary>
public sealed class MiniEtlScenario : ScenarioBase
{
    private readonly IMongoCollection<BsonDocument> _sourceCollection;
    private readonly IMongoCollection<BsonDocument> _lookupCollection;
    private readonly IMongoCollection<BsonDocument> _writeCollection;
    private readonly int _seedCount;

    public MiniEtlScenario(
        IMongoCollection<BsonDocument> sourceCollection,
        IMongoCollection<BsonDocument> lookupCollection,
        IMongoCollection<BsonDocument> writeCollection,
        int seedCount)
    {
        _sourceCollection = sourceCollection;
        _lookupCollection = lookupCollection;
        _writeCollection = writeCollection;
        _seedCount = seedCount;
    }

    public override string Name => "MiniETL";

    protected override async Task<bool> ExecuteOperationAsync(int iteration, CancellationToken ct)
    {
        // 1. Read from source
        var sourceId = $"bench-{iteration % _seedCount:D8}";
        var sourceDoc = await _sourceCollection
            .Find(Builders<BsonDocument>.Filter.Eq("_id", sourceId))
            .FirstOrDefaultAsync(ct);

        if (sourceDoc is null) return false;

        // 2. Lookup reference
        var refId = sourceDoc.GetValue("referenceId", BsonNull.Value);
        BsonDocument? refDoc = null;
        if (refId != BsonNull.Value)
        {
            refDoc = await _lookupCollection
                .Find(Builders<BsonDocument>.Filter.Eq("_id", refId))
                .FirstOrDefaultAsync(ct);
        }

        // 3. Transform and write
        var output = new BsonDocument
        {
            { "_id", $"etl-{iteration:D8}" },
            { "sourceId", sourceId },
            { "hasReference", refDoc is not null },
            { "processedAt", DateTime.UtcNow },
            { "combinedValue", sourceDoc.GetValue("numericValue", 0).ToDouble() + (refDoc?.GetValue("numericValue", 0).ToDouble() ?? 0) }
        };

        var filter = Builders<BsonDocument>.Filter.Eq("_id", output["_id"]);
        await _writeCollection.ReplaceOneAsync(filter, output, new ReplaceOptions { IsUpsert = true }, ct);
        return true;
    }
}
