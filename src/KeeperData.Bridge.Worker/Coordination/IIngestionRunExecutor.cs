using KeeperData.Core.Locking;

namespace KeeperData.Bridge.Worker.Coordination;

/// <summary>
/// Runs an ingestion while keeping the already-acquired run lock alive. Split out from the
/// coordinator because it is timing/threading-bound (periodic lock renewal, background dispatch)
/// and is exercised by integration tests rather than unit tests.
/// </summary>
public interface IIngestionRunExecutor
{
    /// <summary>Runs the import inline, renewing the lock periodically until it completes.</summary>
    Task RunWithRenewalAsync(IDistributedLockHandle lockHandle, Guid runId, string sourceType, CancellationToken cancellationToken);

    /// <summary>Runs the import on a background thread; takes ownership of disposing the lock handle.</summary>
    void StartInBackground(IDistributedLockHandle lockHandle, Guid runId, string sourceType, CancellationToken cancellationToken);
}
