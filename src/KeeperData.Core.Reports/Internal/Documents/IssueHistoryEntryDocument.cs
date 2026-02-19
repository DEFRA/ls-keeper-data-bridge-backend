using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace KeeperData.Core.Reports.Internal.Documents;

/// <summary>
/// Persistence document for an issue history/lineage entry.
/// Anti-corruption layer between domain and MongoDB.
/// </summary>
internal class IssueHistoryEntryDocument
{
    [BsonId] public string Id { get; set; } = string.Empty;
    [BsonElement("issue_id")] public string IssueId { get; set; } = string.Empty;
    [BsonElement("action")] public string Action { get; set; } = string.Empty;
    [BsonElement("performed_by")] public string PerformedBy { get; set; } = "system";
    [BsonElement("detail")][BsonIgnoreIfNull] public string? Detail { get; set; }
    [BsonElement("occurred_at")] public DateTime OccurredAtUtc { get; set; }
}
