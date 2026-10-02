namespace KeeperData.Bridge.Worker.Coordination;

/// <summary>
/// Owns the mutual-exclusion lock for the ETL pipeline and dispatches its runs.
/// Separate from <see cref="IIngestionRunCoordinator"/>, which owns the legacy Mongo import, so the
/// two triggers cannot affect one another.
/// </summary>
public interface IEtlImportCoordinator
{
    /// <param name="dataset">Restricts the run to one dataset, or null for all of them.</param>
    /// <param name="rebuild">Clears every stage before the run, so it rebuilds from the source files
    /// rather than reusing the artefacts already written.</param>
    Task<EtlImportStartResult> StartAsync(string sourceType, string? dataset, bool rebuild = false, CancellationToken cancellationToken = default);
}

/// <summary>Either the id of the import that was started, the id of the one already in flight that
/// stopped it - a caller that collides can then poll the run it collided with - or why a requested
/// rebuild could not clear stage storage.</summary>
/// <param name="ClearedStages">Stages the failed rebuild had already emptied before it stopped.</param>
public sealed record EtlImportStartResult(
    Guid? ImportId,
    Guid? InFlightImportId,
    string? RebuildError = null,
    IReadOnlyList<string>? ClearedStages = null)
{
    public bool Accepted => ImportId.HasValue;

    public static EtlImportStartResult Started(Guid importId) => new(importId, null);

    public static EtlImportStartResult Conflict(Guid? inFlightImportId) => new(null, inFlightImportId);

    public static EtlImportStartResult RebuildFailed(string error, IReadOnlyList<string>? clearedStages = null)
        => new(null, null, error, clearedStages);
}
