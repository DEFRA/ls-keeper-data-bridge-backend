using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.EtlPipeline.Payloads;

/// <summary>One folded Parquet snapshot per dataset in snapshots/. Output of snapshot, input to load.</summary>
public sealed record SnapshotFile(DataSetDefinition Definition)
{
    public Guid RunId { get; init; }

    public string Key { get; init; } = string.Empty;

    public string SourceKey { get; init; } = string.Empty;

    public DateTimeOffset Timestamp { get; init; }

    public bool Created { get; init; }
}
