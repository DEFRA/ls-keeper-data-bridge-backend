using KeeperData.Core.ETL.Models;

namespace KeeperData.Core.ETL.Export;

public interface ICphExportStatusService
{
    Task<CphExportStatus> CreateAsync(Guid exportId, string sourceDuckDbPath, CancellationToken cancellationToken = default);
    Task<CphExportStatus?> GetAsync(Guid exportId, CancellationToken cancellationToken = default);
    Task UpdateAsync(CphExportStatus status, CancellationToken cancellationToken = default);
    Task<CphExportStatus?> GetLatestRunningAsync(CancellationToken cancellationToken = default);
}
