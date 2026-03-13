using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Cleanse.Export.Command.Domain;

namespace KeeperData.Core.Reports.Cleanse.Export.Command.AggregateRoots;

/// <summary>
/// Aggregate root representing an ad-hoc cleanse report export operation.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Aggregate root - covered by unit and integration tests.")]
public class CleanseExportOperation
{
    public required string Id { get; set; }
    public CleanseExportStatus Status { get; set; }
    public DateTime StartedAtUtc { get; set; }
    public DateTime? CompletedAtUtc { get; set; }
    public double ProgressPercentage { get; set; }
    public string StatusDescription { get; set; } = string.Empty;
    public int TotalRecords { get; set; }
    public int RecordsExported { get; set; }
    public string? ReportObjectKey { get; set; }
    public string? ReportUrl { get; set; }
    public string? Error { get; set; }
    public long? DurationMs { get; set; }

    /// <summary>
    /// Creates a new export operation in the Pending state.
    /// </summary>
    public static CleanseExportOperation Create() => new()
    {
        Id = Guid.NewGuid().ToString(),
        Status = CleanseExportStatus.Pending,
        StartedAtUtc = DateTime.UtcNow,
        StatusDescription = "Export pending..."
    };

    /// <summary>
    /// Transitions the operation to Running.
    /// </summary>
    public void Start()
    {
        Status = CleanseExportStatus.Running;
        StatusDescription = "Export running...";
    }

    /// <summary>
    /// Updates the progress counters.
    /// </summary>
    public void UpdateProgress(int recordsExported, int totalRecords, string description)
    {
        RecordsExported = recordsExported;
        TotalRecords = totalRecords;
        ProgressPercentage = totalRecords > 0
            ? Math.Round(100.0 * recordsExported / totalRecords, 2)
            : 0;
        StatusDescription = description;
    }

    /// <summary>
    /// Marks the operation as completed.
    /// </summary>
    public void Complete(long durationMs)
    {
        Status = CleanseExportStatus.Completed;
        CompletedAtUtc = DateTime.UtcNow;
        ProgressPercentage = 100.0;
        StatusDescription = "Export completed";
        DurationMs = durationMs;
    }

    /// <summary>
    /// Marks the operation as failed.
    /// </summary>
    public void Fail(string error, long durationMs)
    {
        Status = CleanseExportStatus.Failed;
        CompletedAtUtc = DateTime.UtcNow;
        StatusDescription = "Export failed";
        Error = error;
        DurationMs = durationMs;
    }

    /// <summary>
    /// Sets the S3 report details after a successful upload.
    /// </summary>
    public void SetReportDetails(string objectKey, string reportUrl)
    {
        ReportObjectKey = objectKey;
        ReportUrl = reportUrl;
    }

    /// <summary>
    /// Updates just the presigned URL (for regeneration).
    /// </summary>
    public void UpdateReportUrl(string reportUrl)
    {
        ReportUrl = reportUrl;
    }
}
