using KeeperData.Core.Reports.Cleanse.Export.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Export.Operations.Dtos;

namespace KeeperData.Core.Reports.Cleanse.Export.Command.Abstract;

/// <summary>
/// Service for managing ad-hoc full cleanse report exports.
/// </summary>
public interface ICleanseExportCommandService
{
    /// <summary>
    /// Starts an ad-hoc full export in the background.
    /// Returns the created export operation, or null if another export is already running (lock not acquired).
    /// </summary>
    Task<CleanseExportOperationDto?> StartFullExportAsync(CancellationToken ct = default);

    /// <summary>
    /// Gets the full details of an export operation by its identifier.
    /// </summary>
    Task<CleanseExportOperationDto?> GetExportOperationAsync(string exportId, CancellationToken ct = default);

    /// <summary>
    /// Gets a paginated list of export operations in reverse chronological order.
    /// </summary>
    Task<IReadOnlyList<CleanseExportOperationSummaryDto>> GetExportOperationsAsync(int skip, int top, CancellationToken ct = default);

    /// <summary>
    /// Regenerates the presigned URL for an export operation's report.
    /// </summary>
    Task<RegenerateReportUrlResult> RegenerateExportUrlAsync(string exportId, CancellationToken ct = default);
}
