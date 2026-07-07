using KeeperData.Core.Ingestion.Models;

namespace KeeperData.Core.Ingestion.Payloads;

/// <summary>One folded Parquet snapshot per dataset in snapshots/.</summary>
public sealed record SnapshotFile(
    DataSetDefinition Dataset,
    StorageLocation Location);
