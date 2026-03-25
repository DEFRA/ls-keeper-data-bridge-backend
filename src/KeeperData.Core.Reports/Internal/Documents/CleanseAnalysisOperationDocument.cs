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
    [BsonElement("final_average_rpm")] public double? FinalAverageRpm { get; set; }
    [BsonElement("cancellation_requested")] public bool CancellationRequested { get; set; }
    [BsonElement("cancelled_at_utc")] public DateTime? CancelledAtUtc { get; set; }
    [BsonElement("progress")][BsonIgnoreIfNull] public OperationNodeDocument? Progress { get; set; }
}

/// <summary>
/// Embedded sub-document representing a node in the unified operation tree.
/// Combines timing, progress, and rate metrics.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Internal persistence document - covered by integration tests.")]
internal class OperationNodeDocument
{
    [BsonElement("name")] public string Name { get; set; } = string.Empty;
    [BsonElement("status")] public string Status { get; set; } = "not-started";
    [BsonElement("description")][BsonIgnoreIfNull] public string? Description { get; set; }
    [BsonElement("percent_complete")][BsonIgnoreIfNull] public double? PercentComplete { get; set; }
    [BsonElement("processed_count")][BsonIgnoreIfNull] public int? ProcessedCount { get; set; }
    [BsonElement("total_records")][BsonIgnoreIfNull] public int? TotalRecords { get; set; }
    [BsonElement("elapsed_ms")] public long ElapsedMs { get; set; }
    [BsonElement("elapsed")] public string Elapsed { get; set; } = string.Empty;
    [BsonElement("projected_remaining_ms")][BsonIgnoreIfNull] public long? ProjectedRemainingMs { get; set; }
    [BsonElement("projected_end_time_utc")][BsonIgnoreIfNull] public DateTime? ProjectedEndTimeUtc { get; set; }
    [BsonElement("current_rpm")][BsonIgnoreIfNull] public double? CurrentRecordsPerMinute { get; set; }
    [BsonElement("average_rpm")][BsonIgnoreIfNull] public double? AverageRecordsPerMinute { get; set; }
    [BsonElement("children")][BsonIgnoreIfNull] public List<OperationNodeDocument>? Children { get; set; }
}
