using Microsoft.Extensions.Logging;

namespace KeeperData.Bridge.Worker.Tasks.Implementations;

public class TaskProcessBulkFiles(ILogger<TaskProcessBulkFiles> logger) : ITaskProcessBulkFiles
{
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("TaskProcessBulkFiles started at {startTime}", DateTime.UtcNow);

        await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);

        logger.LogInformation("TaskProcessBulkFiles completed at {endTime}", DateTime.UtcNow);
    }
}