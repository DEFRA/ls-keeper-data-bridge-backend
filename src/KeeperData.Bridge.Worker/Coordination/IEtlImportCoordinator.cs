namespace KeeperData.Bridge.Worker.Coordination;

/// <summary>
/// Owns the mutual-exclusion lock for the ETL pipeline and dispatches its runs.
/// Separate from <see cref="IIngestionRunCoordinator"/>, which owns the legacy Mongo import, so the
/// two triggers cannot affect one another.
/// </summary>
public interface IEtlImportCoordinator
{
    /// <param name="dataset">Restricts the run to one dataset, or null for all of them.</param>
    Task<EtlImportStartResult> StartAsync(string sourceType, string? dataset, CancellationToken cancellationToken = default);
}

/// <summary>Either the id of the import that was started, or the id of the one already in flight
/// that stopped it - a caller that collides can then poll the run it collided with.</summary>
public sealed record EtlImportStartResult(Guid? ImportId, Guid? InFlightImportId)
{
    public bool Accepted => ImportId.HasValue;

    public static EtlImportStartResult Started(Guid importId) => new(importId, null);

    public static EtlImportStartResult Conflict(Guid? inFlightImportId) => new(null, inFlightImportId);
}
