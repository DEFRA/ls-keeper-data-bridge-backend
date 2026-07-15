using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.EtlPipeline.Payloads;

/// <summary>One folded Parquet snapshot per dataset in snapshots/. Output of snapshot, input to load.</summary>
public sealed record SnapshotFile(DataSetDefinition Definition)
{
    public Guid RunId { get; init; }

    /* Delete this region once the previous stage provides these */
    #region TEMP - PlaceholderInputs

    public IReadOnlyList<string> Files { get; init; } = [];

    #endregion
}
