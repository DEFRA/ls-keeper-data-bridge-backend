using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.EtlPipeline.Snapshots;

public sealed record DeltaMergeResult
{
    public int DeltasApplied { get; init; }
    public long RowsUpserted { get; init; }
    public long RowsIgnoredDeletes { get; init; }
    public long RowsRejected { get; init; }
    public long RowCount { get; init; }
    public IReadOnlyList<string> ColumnsNullified { get; init; } = [];
    public IReadOnlyList<string> ColumnsAdded { get; init; } = [];
}
