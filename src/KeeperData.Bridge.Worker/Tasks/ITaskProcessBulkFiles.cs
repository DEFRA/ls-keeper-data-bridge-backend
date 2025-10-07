namespace KeeperData.Bridge.Worker.Tasks;

public interface ITaskProcessBulkFiles : ITask
{
    /// <summary>
    /// Starts the import process asynchronously and returns immediately after acquiring the lock.
    /// The import continues running in the background.
    /// </summary>
    /// <param name="sourceType">The source type for the import (e.g., "internal" or "external")</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>The import ID if lock was acquired successfully, null if lock could not be acquired</returns>
    Task<Guid?> StartAsync(string sourceType, CancellationToken cancellationToken = default);
}