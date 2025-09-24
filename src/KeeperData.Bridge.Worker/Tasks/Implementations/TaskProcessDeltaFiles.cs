using Microsoft.Extensions.Logging;

namespace KeeperData.Bridge.Worker.Tasks.Implementations;

public class TaskProcessDeltaFiles(ILogger<TaskProcessDeltaFiles> logger) : ITaskProcessDeltaFiles
{
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("TaskProcessDeltaFiles started at {startTime}", DateTime.UtcNow);

        await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);

        logger.LogInformation("TaskProcessDeltaFiles completed at {endTime}", DateTime.UtcNow);
    }
}