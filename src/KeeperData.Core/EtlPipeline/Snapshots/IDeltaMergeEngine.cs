using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.EtlPipeline.Snapshots;

public interface IDeltaMergeEngine
{
    Task<DeltaMergeResult> MergeAsync(
        DataSetDefinition definition,
        DeltaMergeSource? baseSnapshot,
        IReadOnlyList<DeltaMergeSource> deltas,
        Stream output,
        CancellationToken cancellationToken = default);
}
