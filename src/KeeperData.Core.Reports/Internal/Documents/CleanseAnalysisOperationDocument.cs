using System.Diagnostics.CodeAnalysis;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace KeeperData.Core.Reports.Internal.Documents;

/// <summary>
/// Persistence document for a cleanse analysis operation.
/// Anti-corruption layer between domain and MongoDB.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Internal persistence document - covered by integration tests.")]
internal class CleanseAnalysisOperationDocument
{
    [BsonId] public string Id { get; set; } = string.Empty;
    [BsonElement("status")] public string Status { get; set; } = string.Empty;
    [BsonElement("started_at_utc")] public DateTime StartedAtUtc { get; set; }
    [BsonElement("completed_at_utc")] public DateTime? CompletedAtUtc { get; set; }
    [BsonElement("progress_percentage")] public double ProgressPercentage { get; set; }
    [BsonElement("status_description")] public string StatusDescription { get; set; } = string.Empty;
    [BsonElement("records_analyzed")] public int RecordsAnalyzed { get; set; }
    [BsonElement("total_records")] public int TotalRecords { get; set; }
    [BsonElement("issues_found")] public int IssuesFound { get; set; }
    [BsonElement("issues_resolved")] public int IssuesResolved { get; set; }
    [BsonElement("error")] public string? Error { get; set; }
    [BsonElement("duration_ms")] public long? DurationMs { get; set; }
    [BsonElement("report_object_key")] public string? ReportObjectKey { get; set; }
    [BsonElement("report_url")] public string? ReportUrl { get; set; }
}
