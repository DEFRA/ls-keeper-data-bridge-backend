using System.Diagnostics.CodeAnalysis;
using MongoDB.Bson.Serialization.Attributes;

namespace KeeperData.Core.Reports.Internal.Documents;

/// <summary>
/// Persistence document for an issue.
/// Anti-corruption layer between domain and MongoDB.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Internal persistence document - covered by integration tests.")]
internal class IssueDocument
{
    [BsonId] public string Id { get; set; } = string.Empty;
    [BsonElement("operation_id")] public string OperationId { get; set; } = string.Empty;
    [BsonElement("issue_code")] public string IssueCode { get; set; } = string.Empty;
    [BsonElement("rule_code")] public string RuleCode { get; set; } = string.Empty;
    [BsonElement("error_code")] public string ErrorCode { get; set; } = string.Empty;
    [BsonElement("error_description")] public string ErrorDescription { get; set; } = string.Empty;
    [BsonElement("cts_lid_full_identifier")] public string CtsLidFullIdentifier { get; set; } = string.Empty;
    [BsonElement("cph")] public string Cph { get; set; } = string.Empty;
    [BsonElement("created_at")] public DateTime CreatedAtUtc { get; set; }
    [BsonElement("last_updated_at")] public DateTime LastUpdatedAtUtc { get; set; }
    [BsonElement("is_active")] public bool IsActive { get; set; }
    [BsonElement("is_ignored")] public bool IsIgnored { get; set; }
    [BsonElement("resolution_status")] public string ResolutionStatus { get; set; } = "None";
    [BsonElement("assigned_to")][BsonIgnoreIfNull] public string? AssignedTo { get; set; }
    [BsonElement("email_cts")][BsonIgnoreIfNull] public string[]? EmailCTS { get; set; }
    [BsonElement("email_sam")][BsonIgnoreIfNull] public string? EmailSAM { get; set; }
    [BsonElement("tel_cts")][BsonIgnoreIfNull] public string[]? TelCTS { get; set; }
    [BsonElement("tel_sam")][BsonIgnoreIfNull] public string? TelSAM { get; set; }
    [BsonElement("fsa")][BsonIgnoreIfNull] public string? FSA { get; set; }
}
