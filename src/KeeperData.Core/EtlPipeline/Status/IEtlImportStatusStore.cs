namespace KeeperData.Core.EtlPipeline.Status;

/// <summary>Persistence for ETL import status, so a run survives an API restart and can
/// be polled by id.</summary>
public interface IEtlImportStatusStore
{
    Task CreateQueuedAsync(Guid importId, string sourceType, string? dataset, CancellationToken cancellationToken);

    Task MarkRunningAsync(Guid importId, IReadOnlyList<string> stageNames, CancellationToken cancellationToken);

    /// <summary>Records a completed stage and whatever it produced, and extends the lease.</summary>
    Task RecordStageAsync(Guid importId, EtlImportStageProgress progress, CancellationToken cancellationToken);

    Task MarkSucceededAsync(Guid importId, CancellationToken cancellationToken);

    Task MarkFailedAsync(Guid importId, string error, CancellationToken cancellationToken);

    /// <summary>The import, or null if the id is unknown. A run whose lease has lapsed is returned
    /// as failed.</summary>
    Task<EtlImportDocument?> GetAsync(Guid importId, CancellationToken cancellationToken);

    /// <summary>The import currently holding the pipeline, if any. Used to tell a caller which run
    /// it collided with rather than only that it collided.</summary>
    Task<EtlImportDocument?> GetInFlightAsync(CancellationToken cancellationToken);

    /// <summary>A page of imports, most recently requested first, so a caller that has lost an
    /// import id can still find its run. Runs whose lease has lapsed are returned as failed.</summary>
    Task<EtlImportPage> ListAsync(int skip, int top, CancellationToken cancellationToken);
}

/// <summary>One page of imports, with the total available so a caller can paginate.</summary>
public sealed record EtlImportPage(IReadOnlyList<EtlImportDocument> Imports, long TotalCount);

/// <summary>One stage's outcome, already mapped out of the pipeline's payload types.</summary>
public sealed record EtlImportStageProgress(
    string StageName,
    int ItemCount,
    TimeSpan Elapsed,
    IReadOnlyList<EtlImportDatasetProgress> Datasets,
    string? DuckDbKey = null);

/// <summary>What a stage produced for one dataset. Every field is optional: a stage only fills in
/// the part of the picture it owns, and the store merges them.</summary>
public sealed record EtlImportDatasetProgress(string Dataset)
{
    public IReadOnlyList<(string Key, long Size)> SourceFiles { get; init; } = [];
    public IReadOnlyList<string> RawKeys { get; init; } = [];
    public IReadOnlyList<string> NormalisedKeys { get; init; } = [];
    public string? SnapshotKey { get; init; }
    public DateTimeOffset? SnapshotSourceTimestamp { get; init; }
    public long? RowCount { get; init; }
    public long? RowsUpserted { get; init; }
    public long? RowsIgnoredDeletes { get; init; }
}
