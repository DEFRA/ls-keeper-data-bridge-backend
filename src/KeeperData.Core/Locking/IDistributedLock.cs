namespace KeeperData.Core.Locking;

public interface IDistributedLock
{
    Task<IDistributedLockHandle?> TryAcquireAsync(string lockName, TimeSpan duration, CancellationToken cancellationToken = default);
}