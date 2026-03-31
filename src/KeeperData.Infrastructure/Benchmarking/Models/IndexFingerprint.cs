using MongoDB.Bson;

namespace KeeperData.Infrastructure.Benchmarking.Models;

/// <summary>
/// Fingerprint of an index on a benchmark collection.
/// </summary>
public sealed record IndexFingerprint
{
    public string CollectionName { get; init; } = default!;
    public string IndexName { get; init; } = default!;
    public BsonDocument KeyDefinition { get; init; } = default!;
    public bool IsUnique { get; init; }
}
