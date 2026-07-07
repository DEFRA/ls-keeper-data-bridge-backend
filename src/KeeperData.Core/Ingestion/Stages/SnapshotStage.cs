using KeeperData.Core.Pipeline;
using KeeperData.Core.Ingestion.Payloads;
#pragma warning disable CS9113 // Parameter is unread.

namespace KeeperData.Core.Ingestion.Stages;

/// <summary>Folds a dataset's normalised deltas onto its
/// main (upsert on primary keys, apply I/U, drop D) into one snapshot Parquet. Materialises: snapshots/.</summary>
public sealed class SnapshotStage : MapStage<NormalisedFileSet, SnapshotFile>
{
    public override string Name => "snapshot";

    protected override Task<SnapshotFile> MapAsync(NormalisedFileSet input, IPipelineContext context, CancellationToken cancellationToken)
        // STREAMS: seed from Main -> fold each delta oldest->newest (upsert on PKs / CHANGE_TYPE) -> WriteAtomic to snapshots/
        => throw new NotImplementedException();
}
