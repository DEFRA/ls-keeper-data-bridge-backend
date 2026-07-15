using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.Pipeline;

namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>Folds a dataset's deltas onto its main file (upsert on primary keys, apply CHANGE_TYPE)
/// into one snapshot Parquet. Materialises: snapshots/.
/// STUB - passes through. The owner implements the fold in MapAsync.</summary>
public sealed class SnapshotStage : MapStage<NormalisedFileSet, SnapshotFile>
{
    public override string Name => "snapshot";

    protected override Task<SnapshotFile> MapAsync(NormalisedFileSet input, IPipelineContext context, CancellationToken cancellationToken)
        => Task.FromResult(new SnapshotFile(input.Definition));
}
