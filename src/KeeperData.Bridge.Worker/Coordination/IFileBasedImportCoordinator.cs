namespace KeeperData.Bridge.Worker.Coordination;

/// <summary>
/// Owns the mutual-exclusion lock for the file-based ETL pipeline and dispatches its runs.
/// Separate from <see cref="IIngestionRunCoordinator"/>, which owns the legacy Mongo import, so the
/// two triggers cannot affect one another.
/// </summary>
public interface IFileBasedImportCoordinator
{
    /// <param name="dataset">Restricts the run to one dataset, or null for all of them.</param>
    Task<FileBasedImportStartResult> StartAsync(string sourceType, string? dataset, CancellationToken cancellationToken = default);
}

/// <summary>Either the id of the import that was started, or the id of the one already in flight
/// that stopped it - a caller that collides can then poll the run it collided with.</summary>
public sealed record FileBasedImportStartResult(Guid? ImportId, Guid? InFlightImportId)
{
    public bool Accepted => ImportId.HasValue;

    public static FileBasedImportStartResult Started(Guid importId) => new(importId, null);

    public static FileBasedImportStartResult Conflict(Guid? inFlightImportId) => new(null, inFlightImportId);
}
