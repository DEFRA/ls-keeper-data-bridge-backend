namespace KeeperData.Bridge.Worker.Tasks;

public interface ITaskProcessBulkFiles
{
    /// <summary>
    /// Runs the legacy bulk import for a single run. The run lock and its renewal are owned by
    /// the caller (the ingestion run coordinator); this method assumes the lock is already held.
    /// </summary>
    Task RunImportAsync(Guid importId, string sourceType, CancellationToken cancellationToken);
}
