using KeeperData.Core.Reports.Cleanse.Export.Command.AggregateRoots;

namespace KeeperData.Core.Reports.Cleanse.Export.Operations.Abstract;

/// <summary>
/// Repository for persisting ad-hoc cleanse export operation aggregate roots.
/// </summary>
public interface ICleanseExportOperationRepository
{
    Task CreateAsync(CleanseExportOperation operation, CancellationToken ct = default);
    Task<CleanseExportOperation?> GetByIdAsync(string operationId, CancellationToken ct = default);
    Task UpdateAsync(CleanseExportOperation operation, CancellationToken ct = default);
}
