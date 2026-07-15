using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.Pipeline;

namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>Loads every dataset snapshot as a table into the single DuckDB staging database.
/// All snapshots -> one database. Materialises: staging/.
/// STUB - passes through. The owner adds the DuckDB writer dependency and implements AggregateAsync.</summary>
public sealed class LoadDuckDbStage : AggregateStage<SnapshotFile, StagingDatabase>
{
    public override string Name => "load-duckdb";

    protected override Task<StagingDatabase> AggregateAsync(IReadOnlyList<SnapshotFile> all, IPipelineContext context, CancellationToken cancellationToken)
        => Task.FromResult(new StagingDatabase());
}
