using KeeperData.Core.Ingestion.Contracts;
using KeeperData.Core.Ingestion.Models;
using KeeperData.Core.Ingestion.Payloads;

namespace KeeperData.Bridge.Worker.NewPipelineUsage.Samples;

/// <summary>Sample staging writer: reports where the single DuckDB database would be written.
/// The real implementation runs CREATE TABLE AS SELECT * FROM read_parquet(...) per snapshot.</summary>
public sealed class SampleDuckDbStagingWriter : IDuckDbStagingWriter
{
    public Task<StorageLocation> WriteAsync(IReadOnlyList<SnapshotFile> snapshots, CancellationToken cancellationToken)
        => Task.FromResult(new StorageLocation(BridgeArea.Staging, "keeper_data_bridge.duckdb"));
}
