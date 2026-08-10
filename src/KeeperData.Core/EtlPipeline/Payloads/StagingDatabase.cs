using KeeperData.Core.EtlPipeline.Staging;

namespace KeeperData.Core.EtlPipeline.Payloads;

/// <summary>The single DuckDB staging database in staging/. Final output of the pipeline.</summary>
public sealed record StagingDatabase
{
    public Guid RunId { get; init; }

    public string Key { get; init; } = string.Empty;

    /// <summary>The newest snapshot source timestamp the database holds, carried through from the
    /// snapshots loaded. Not the time the ETL ran.</summary>
    public DateTimeOffset SourceTimestamp { get; init; }

    public IReadOnlyList<StagingTable> Tables { get; init; } = [];

    /// <summary>False when the database for these snapshots already existed and was reused.</summary>
    public bool Created { get; init; }
}
