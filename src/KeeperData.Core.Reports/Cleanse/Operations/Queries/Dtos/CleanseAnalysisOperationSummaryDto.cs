using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;

namespace KeeperData.Core.Reports.Cleanse.Operations.Queries.Dtos;

/// <summary>
/// Summary DTO for listing cleanse analysis operations.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO class - no logic to test.")]
public class CleanseAnalysisOperationSummaryDto
{
    /// <summary>
    /// Gets or sets the unique operation identifier.
    /// </summary>
    public required string Id { get; set; }

    /// <summary>
    /// Gets or sets the current status of the operation.
    /// </summary>
    public required string Status { get; set; }

    /// <summary>
    /// Gets or sets the UTC timestamp when the operation started.
    /// </summary>
    public DateTime StartedAtUtc { get; set; }

    /// <summary>
    /// Gets or sets the UTC timestamp when the operation completed.
    /// </summary>
    public DateTime? CompletedAtUtc { get; set; }

    /// <summary>
    /// Gets or sets the progress percentage (0-100).
    /// </summary>
    public double ProgressPercentage { get; set; }

    /// <summary>
    /// Gets or sets the number of records analyzed.
    /// </summary>
    public int RecordsAnalyzed { get; set; }

    /// <summary>
    /// Gets or sets the total number of records to analyze.
    /// </summary>
    public int TotalRecords { get; set; }

    /// <summary>
    /// Gets or sets the number of issues found.
    /// </summary>
    public int IssuesFound { get; set; }

    /// <summary>
    /// Gets or sets the number of issues resolved.
    /// </summary>
    public int IssuesResolved { get; set; }

    /// <summary>
    /// Gets or sets the total duration in milliseconds.
    /// </summary>
    public long? DurationMs { get; set; }

    /// <summary>
    /// Gets or sets the S3 object key for the generated report.
    /// </summary>
    public string? ReportObjectKey { get; set; }

    /// <summary>
    /// Gets or sets the presigned URL to download the generated report.
    /// </summary>
    public string? ReportUrl { get; set; }

    /// <summary>
    /// Gets or sets the final average records per minute when the operation completed.
    /// Null while the operation is still running.
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public double? FinalAverageRpm { get; set; }

    /// <summary>
    /// Gets or sets the UTC timestamp when the operation was cancelled.
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTime? CancelledAtUtc { get; set; }

    /// <summary>
    /// Gets or sets the name of the currently executing phase.
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? CurrentPhase { get; set; }

    /// <summary>
    /// Gets or sets live performance statistics. Only populated for running operations.
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public CleanseRunStatsDto? Stats { get; set; }
}
