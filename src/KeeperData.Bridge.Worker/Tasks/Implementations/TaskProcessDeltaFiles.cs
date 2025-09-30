using KeeperData.Core.Locking;
using Microsoft.Extensions.Logging;

namespace KeeperData.Bridge.Worker.Tasks.Implementations;

public class TaskProcessDeltaFiles(ILogger<TaskProcessDeltaFiles> logger, IDistributedLock distributedLock) : ITaskProcessDeltaFiles
{
    private const string LockName = nameof(TaskProcessDeltaFiles);

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Attempting to acquire lock for {LockName}.", LockName);

        using var _ = await distributedLock.TryAcquireAsync(LockName, TimeSpan.FromMinutes(30), cancellationToken);

        if (_ == null)
        {
            logger.LogInformation("Could not acquire lock for {LockName}, another instance is likely running.", LockName);
            return;
        }

        logger.LogInformation("Lock acquired for {LockName}. Task started at {startTime}", LockName, DateTime.UtcNow);

        await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);

        logger.LogInformation("TaskProcessDeltaFiles completed at {endTime}", DateTime.UtcNow);
    }
}