using KeeperData.Core.Reports.Cleanse.Export.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Export.Command.Results;

namespace KeeperData.Core.Reports.Cleanse.Export.Command.Abstract;

/// <summary>
/// Service for exporting cleanse reports to CSV and uploading to S3.
/// </summary>
public interface ICleanseReportExportCommandService
{
    /// <summary>
    /// Exports the cleanse report for the given analysis operation: streams issues to CSV, zips, uploads to S3,
    /// sets report details on the operation, and optionally sends a notification.
    /// </summary>
    /// <param name="operationId">The analysis operation identifier.</param>
    /// <param name="options">Export options controlling date filtering and notification behaviour.</param>
    /// <param name="onProgress">Optional callback invoked with (recordsProcessed, totalRecords, stepDescription).</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>True if the export completed successfully; false otherwise.</returns>
    Task<bool> ExportReportAsync(string operationId, ExportOptions options, Func<int, int, string, Task>? onProgress, CancellationToken ct);

    /// <summary>
    /// Core export mechanics: streams issues to CSV, zips, uploads to S3 and returns the result.
    /// Does not record side effects on any operation document.
    /// </summary>
    /// <param name="options">Export options controlling date filtering.</param>
    /// <param name="onProgress">Optional callback invoked with (recordsProcessed, totalRecords, stepDescription).</param>
    /// <param name="ct">Cancellation token.</param>
    Task<CleanseReportExportResult> ExportToStorageAsync(ExportOptions options, Func<int, int, string, Task>? onProgress, CancellationToken ct);

    /// <summary>
    /// Regenerates the presigned URL for an analysis operation's report.
    /// </summary>
    Task<RegenerateReportUrlResult> RegenerateReportUrlAsync(string operationId, CancellationToken ct = default);

    /// <summary>
    /// Gets the UTC timestamp of the last successful incremental export, or null if none has occurred.
    /// </summary>
    Task<DateTime?> GetLastExportedAtUtcAsync(CancellationToken ct = default);

    /// <summary>
    /// Records the current UTC time as the last successful incremental export timestamp.
    /// </summary>
    Task RecordSuccessfulExportAsync(CancellationToken ct = default);
}
