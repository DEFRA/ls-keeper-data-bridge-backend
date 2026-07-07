using KeeperData.Core.Ingestion.Models;

namespace KeeperData.Core.Ingestion.Payloads;

/// <summary>The single DuckDB staging database in staging/.</summary>
public sealed record StagingDatabase(
    StorageLocation Location);
