namespace KeeperData.Core.Locking;

public interface IDistributedLock
{
    Task<IDisposable?> TryAcquireAsync(string lockName, TimeSpan duration, CancellationToken cancellationToken = default);
}