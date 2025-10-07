namespace KeeperData.Core.Locking;

/// <summary>
/// Represents a handle to a distributed lock that supports renewal and async disposal.
/// </summary>
public interface IDistributedLockHandle : IAsyncDisposable
{
    /// <summary>
    /// Attempts to renew the lock by extending its expiration time.
    /// </summary>
    /// <param name="extension">The additional time to extend the lock</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if the lock was successfully renewed, false if the lock is no longer owned by this handle</returns>
    Task<bool> TryRenewAsync(TimeSpan extension, CancellationToken cancellationToken = default);
}