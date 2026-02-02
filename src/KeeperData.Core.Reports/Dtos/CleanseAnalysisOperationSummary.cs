namespace KeeperData.Core.Reports.Dtos;

/// <summary>
/// Summary DTO for listing cleanse analysis operations.
/// </summary>
public class CleanseAnalysisOperationSummary
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
}
