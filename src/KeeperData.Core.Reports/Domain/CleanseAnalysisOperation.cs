namespace KeeperData.Core.Reports.Domain;

/// <summary>
/// Represents the status of a cleanse analysis operation.
/// </summary>
public enum CleanseAnalysisStatus
{
    /// <summary>
    /// The operation has not started yet.
    /// </summary>
    NotStarted,

    /// <summary>
    /// The operation is currently running.
    /// </summary>
    Running,

    /// <summary>
    /// The operation completed successfully.
    /// </summary>
    Completed,

    /// <summary>
    /// The operation failed with an error.
    /// </summary>
    Failed,

    /// <summary>
    /// The operation was cancelled.
    /// </summary>
    Cancelled
}

/// <summary>
/// Represents a cleanse analysis operation with its metadata and progress.
/// </summary>
public class CleanseAnalysisOperation
{
    /// <summary>
    /// Gets or sets the unique operation identifier.
    /// </summary>
    public required string Id { get; set; }

    /// <summary>
    /// Gets or sets the current status of the operation.
    /// </summary>
    public CleanseAnalysisStatus Status { get; set; }

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
    /// Gets or sets a human-readable status description.
    /// </summary>
    public string StatusDescription { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the number of records analyzed so far.
    /// </summary>
    public int RecordsAnalyzed { get; set; }

    /// <summary>
    /// Gets or sets the total number of records to analyze.
    /// </summary>
    public int TotalRecords { get; set; }

    /// <summary>
    /// Gets or sets the number of issues found during analysis.
    /// </summary>
    public int IssuesFound { get; set; }

    /// <summary>
    /// Gets or sets the number of previously active issues that were resolved.
    /// </summary>
    public int IssuesResolved { get; set; }

    /// <summary>
    /// Gets or sets the error message if the operation failed.
    /// </summary>
    public string? Error { get; set; }

    /// <summary>
    /// Gets or sets the total duration in milliseconds when complete.
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
