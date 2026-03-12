using KeeperData.Core.Reports.Cleanse.Export.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Export.Command.Results;

namespace KeeperData.Core.Reports.Cleanse.Export.Command.Abstract;

/// <summary>
/// Service for exporting cleanse reports to CSV and uploading to S3.
/// </summary>
public interface ICleanseReportExportCommandService
{
    /// <summary>
    /// Exports the cleanse report for the given operation: streams issues to CSV, zips, uploads to S3, sends notification.
    /// </summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="onProgress">Optional callback invoked with (recordsProcessed, totalRecords, stepDescription).</param>
    /// <param name="ct">Cancellation token.</param>
    Task ExportReportAsync(string operationId, Func<int, int, string, Task>? onProgress, CancellationToken ct);
    Task<RegenerateReportUrlResult> RegenerateReportUrlAsync(string operationId, CancellationToken ct = default);
}
