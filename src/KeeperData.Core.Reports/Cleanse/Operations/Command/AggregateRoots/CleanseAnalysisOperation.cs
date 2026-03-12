using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Operations.Queries.Dtos;

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
    /// Gets or sets whether cancellation has been requested for this operation.
    /// </summary>
    public bool CancellationRequested { get; set; }

    /// <summary>
    /// Gets or sets the UTC timestamp when the operation was cancelled.
    /// </summary>
    public DateTime? CancelledAtUtc { get; set; }

    /// <summary>
    /// Gets or sets the final average records per minute when the operation completed.
    /// </summary>
    public double? FinalAverageRpm { get; set; }

    /// <summary>
    /// Gets or sets the name of the currently executing phase.
    /// </summary>
    public string? CurrentPhase { get; set; }

    /// <summary>
    /// Gets or sets the per-phase progress tracking list.
    /// </summary>
    public List<OperationPhaseProgress> Phases { get; set; } = [];

    #region Phase weights for aggregate progress calculation

    private static readonly Dictionary<string, double> PhaseWeights = new()
    {
        [OperationPhase.Analysis.ToString()] = 0.80,
        [OperationPhase.Deactivation.ToString()] = 0.10,
        [OperationPhase.Export.ToString()] = 0.10,
    };

    #endregion

    /// <summary>
    /// Creates a new operation in the Running state.
    /// </summary>
    public static CleanseAnalysisOperation Create(int totalRecords = 0) => new()
    {
        Id = Guid.NewGuid().ToString(),
        Status = CleanseAnalysisStatus.Running,
        StartedAtUtc = DateTime.UtcNow,
        TotalRecords = totalRecords,
        StatusDescription = "Initializing analysis...",
        Phases =
        [
            new() { Name = OperationPhase.Analysis.ToString() },
            new() { Name = OperationPhase.Deactivation.ToString() },
            new() { Name = OperationPhase.Export.ToString() },
        ]
    };

    /// <summary>
    /// Updates the progress of this operation.
    /// </summary>
    public void UpdateProgress(
        double progressPercentage,
        string statusDescription,
        int recordsAnalyzed,
        int totalRecords,
        int issuesFound,
        int issuesResolved)
    {
        ProgressPercentage = progressPercentage;
        StatusDescription = statusDescription;
        RecordsAnalyzed = recordsAnalyzed;
        TotalRecords = totalRecords;
        IssuesFound = issuesFound;
        IssuesResolved = issuesResolved;
    }

    /// <summary>
    /// Requests cancellation of this operation. Sets the flag that the pump polls
    /// and transitions the status to Cancelling so API consumers can see the pending state.
    /// </summary>
    public void RequestCancellation()
    {
        CancellationRequested = true;
        Status = CleanseAnalysisStatus.Cancelling;
        StatusDescription = "Cancellation requested, waiting for current batch to complete";
    }

    /// <summary>
    /// Marks this operation as cancelled.
    /// </summary>
    public void Cancel(long durationMs)
    {
        Status = CleanseAnalysisStatus.Cancelled;
        CancelledAtUtc = DateTime.UtcNow;
        CompletedAtUtc = DateTime.UtcNow;
        StatusDescription = "Analysis cancelled by user";
        DurationMs = durationMs;

        var durationMinutes = durationMs / 60_000.0;
        FinalAverageRpm = durationMinutes > 0 ? Math.Round(RecordsAnalyzed / durationMinutes, 2) : 0;
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

        var durationMinutes = durationMs / 60_000.0;
        FinalAverageRpm = durationMinutes > 0 ? Math.Round(recordsAnalyzed / durationMinutes, 2) : 0;
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

    /// <summary>
    /// Marks a phase as running and records its start time and total records.
    /// </summary>
    public void StartPhase(OperationPhase phase, int totalRecords)
    {
        var p = GetPhase(phase);
        p.Status = "Running";
        p.StartedAtUtc = DateTime.UtcNow;
        p.TotalRecords = totalRecords;
        CurrentPhase = phase.ToString();
        StatusDescription = $"{phase} phase starting...";
        RecalculateAggregateProgress();
    }

    /// <summary>
    /// Updates the progress counters and description for a specific phase.
    /// </summary>
    public void UpdatePhaseProgress(OperationPhase phase, int recordsProcessed, int totalRecords, string description)
    {
        var p = GetPhase(phase);
        p.RecordsProcessed = recordsProcessed;
        p.TotalRecords = totalRecords;
        p.Percentage = totalRecords > 0 ? Math.Round((double)recordsProcessed / totalRecords * 100, 2) : 0;
        p.Description = description;
        StatusDescription = description;
        RecalculateAggregateProgress();
    }

    /// <summary>
    /// Marks a phase as completed, records its end time and duration.
    /// </summary>
    public void CompletePhase(OperationPhase phase)
    {
        var p = GetPhase(phase);
        p.Status = "Completed";
        p.Percentage = 100.0;
        p.CompletedAtUtc = DateTime.UtcNow;
        p.DurationMs = p.StartedAtUtc.HasValue
            ? (long)(p.CompletedAtUtc.Value - p.StartedAtUtc.Value).TotalMilliseconds
            : null;
        RecalculateAggregateProgress();
    }

    private OperationPhaseProgress GetPhase(OperationPhase phase)
    {
        var name = phase.ToString();
        return Phases.Find(p => p.Name == name)
            ?? throw new InvalidOperationException($"Phase '{name}' not found in operation '{Id}'.");
    }

    private void RecalculateAggregateProgress()
    {
        var aggregate = 0.0;
        foreach (var phase in Phases)
        {
            if (PhaseWeights.TryGetValue(phase.Name, out var weight))
            {
                aggregate += weight * phase.Percentage;
            }
        }
        ProgressPercentage = Math.Round(aggregate, 2);
    }
}