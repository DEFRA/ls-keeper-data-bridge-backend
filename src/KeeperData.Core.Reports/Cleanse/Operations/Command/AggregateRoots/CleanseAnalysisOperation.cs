using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;

namespace KeeperData.Core.Reports.Cleanse.Operations.Command.AggregateRoots;

/// <summary>
/// Agg root representing a cleanse analysis operation, tracking its progress, status, and results.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Aggregate root - covered by integration tests.")]
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

    /// <summary>
    /// Creates a new operation in the Running state.
    /// </summary>
    public static CleanseAnalysisOperation Create(int totalRecords = 0) => new()
    {
        Id = Guid.NewGuid().ToString(),
        Status = CleanseAnalysisStatus.Running,
        StartedAtUtc = DateTime.UtcNow,
        TotalRecords = totalRecords,
        StatusDescription = "Initializing analysis..."
    };

    /// <summary>
    /// Updates the progress of this operation.
    /// </summary>
    public void UpdateProgress(
        double progressPercentage,
        string statusDescription,
        int recordsAnalyzed,
        int issuesFound,
        int issuesResolved)
    {
        ProgressPercentage = progressPercentage;
        StatusDescription = statusDescription;
        RecordsAnalyzed = recordsAnalyzed;
        IssuesFound = issuesFound;
        IssuesResolved = issuesResolved;
    }

    /// <summary>
    /// Marks this operation as completed.
    /// </summary>
    public void Complete(int recordsAnalyzed, int issuesFound, int issuesResolved, long durationMs)
    {
        Status = CleanseAnalysisStatus.Completed;
        CompletedAtUtc = DateTime.UtcNow;
        ProgressPercentage = 100.0;
        StatusDescription = "Analysis completed";
        RecordsAnalyzed = recordsAnalyzed;
        IssuesFound = issuesFound;
        IssuesResolved = issuesResolved;
        DurationMs = durationMs;
    }

    /// <summary>
    /// Marks this operation as failed.
    /// </summary>
    public void Fail(string error, long durationMs)
    {
        Status = CleanseAnalysisStatus.Failed;
        CompletedAtUtc = DateTime.UtcNow;
        StatusDescription = "Analysis failed";
        Error = error;
        DurationMs = durationMs;
    }

    /// <summary>
    /// Sets the report details for this completed operation.
    /// </summary>
    public void SetReportDetails(string objectKey, string reportUrl)
    {
        ReportObjectKey = objectKey;
        ReportUrl = reportUrl;
    }

    /// <summary>
    /// Updates just the report URL (used when regenerating presigned URLs).
    /// </summary>
    public void UpdateReportUrl(string reportUrl)
    {
        ReportUrl = reportUrl;
    }
}