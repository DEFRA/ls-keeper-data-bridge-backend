using KeeperData.Core.Ingestion.Models;
using KeeperData.Core.Ingestion.Payloads;

namespace KeeperData.Core.Ingestion.Contracts;

/// <summary>Loads all per-dataset snapshots into the single DuckDB staging database.</summary>
public interface IDuckDbStagingWriter
{
    Task<StorageLocation> WriteAsync(IReadOnlyList<SnapshotFile> snapshots, CancellationToken cancellationToken);
}
