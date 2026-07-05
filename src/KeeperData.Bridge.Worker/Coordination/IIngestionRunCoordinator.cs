namespace KeeperData.Bridge.Worker.Coordination;

/// <summary>
/// Owns the single mutual-exclusion lock for an ingestion run and dispatches the run.
/// Both triggers — the scheduled job and the API — go through the coordinator, so only one
/// ingestion run can execute at a time regardless of which trigger started it.
/// </summary>
public interface IIngestionRunCoordinator
{
    /// <summary>Runs an ingestion inline (used by the scheduled job). Returns when the run completes.</summary>
    Task RunAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Acquires the lock and starts an ingestion in the background (used by the API).
    /// Returns the run id, or null if the lock could not be acquired.
    /// </summary>
    Task<Guid?> StartAsync(string sourceType, CancellationToken cancellationToken = default);
}
