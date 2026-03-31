using MongoDB.Bson;

namespace KeeperData.Infrastructure.Benchmarking.Models;

/// <summary>
/// Captured explain-plan output for a key query.
/// </summary>
public sealed record ExplainResult
{
    public string QueryName { get; init; } = default!;
    public string WinningPlan { get; init; } = default!;
    public long TotalDocsExamined { get; init; }
    public long TotalKeysExamined { get; init; }
    public long NReturned { get; init; }
    public BsonDocument? RawExplain { get; init; }
}
