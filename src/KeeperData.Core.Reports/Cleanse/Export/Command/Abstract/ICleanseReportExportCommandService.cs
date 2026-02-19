using KeeperData.Core.Reports.Cleanse.Export.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Export.Command.Results;

namespace KeeperData.Core.Reports.Cleanse.Export.Command.Abstract;

/// <summary>
/// Service for exporting cleanse reports to CSV and uploading to S3.
/// </summary>
public interface ICleanseReportExportCommandService
{
    Task ExportReportAsync(string operationId, CancellationToken ct);
    Task<RegenerateReportUrlResult> RegenerateReportUrlAsync(string operationId, CancellationToken ct = default);
}
