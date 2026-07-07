using KeeperData.Core.Pipeline;
using KeeperData.Core.Ingestion.Contracts;
using KeeperData.Core.Ingestion.Payloads;
#pragma warning disable CS9113 // Parameter is unread.

namespace KeeperData.Core.Ingestion.Stages;

/// <summary>Loads every dataset snapshot as a table into the single
/// DuckDB staging database. All snapshots -> one database. Materialises: staging/.</summary>
public sealed class LoadDuckDbStage(IDuckDbStagingWriter stagingWriter) : AggregateStage<SnapshotFile, StagingDatabase>
{
    public override string Name => "load-duckdb";

    protected override Task<StagingDatabase> AggregateAsync(IReadOnlyList<SnapshotFile> all, IPipelineContext context, CancellationToken cancellationToken)
        // STREAMS: for each snapshot -> CREATE TABLE AS SELECT * FROM read_parquet(...) -> one staging/*.duckdb
        => throw new NotImplementedException();
}
