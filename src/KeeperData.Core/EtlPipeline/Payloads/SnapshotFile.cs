using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.EtlPipeline.Payloads;

/// <summary>One folded Parquet snapshot per dataset in snapshots/. Output of snapshot, input to load.</summary>
public sealed record SnapshotFile(DataSetDefinition Definition)
{
    public Guid RunId { get; init; }

    public string Key { get; init; } = string.Empty;

    /// <summary>The latest source timestamp the snapshot includes, read from the name of the newest
    /// file applied. Not the time the ETL ran.</summary>
    public DateTimeOffset SourceTimestamp { get; init; }

    /// <summary>The normalised files this run folded in, oldest first. Empty when the run had nothing
    /// new to apply and reused the existing snapshot.</summary>
    public IReadOnlyList<string> AppliedKeys { get; init; } = [];

    public bool Created { get; init; }

    public long RowCount { get; init; }

    public long RowsUpserted { get; init; }

    /// <summary>Rows carrying CHANGE_TYPE = D. Counted for visibility; deletes are not applied.</summary>
    public long RowsIgnoredDeletes { get; init; }

    public IReadOnlyList<string> ColumnsNullified { get; init; } = [];

    public IReadOnlyList<string> ColumnsAdded { get; init; } = [];
}
