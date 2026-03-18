using KeeperData.Core.Reports.Cleanse.Export.Operations.Dtos;

namespace KeeperData.Core.Reports.Cleanse.Export.Operations.Abstract;

/// <summary>
/// Query service for ad-hoc cleanse export operations.
/// </summary>
public interface ICleanseExportOperationQueries
{
    Task<CleanseExportOperationDto?> GetOperationAsync(string exportId, CancellationToken ct = default);
    Task<IReadOnlyList<CleanseExportOperationSummaryDto>> GetOperationsAsync(int skip, int top, CancellationToken ct = default);
}
