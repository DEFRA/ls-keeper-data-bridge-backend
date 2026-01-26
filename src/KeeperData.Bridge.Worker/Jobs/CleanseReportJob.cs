using KeeperData.Bridge.Worker.Tasks;
using Microsoft.Extensions.Logging;
using Quartz;

namespace KeeperData.Bridge.Worker.Jobs;

[DisallowConcurrentExecution]
public class CleanseReportJob(
    ITaskRunCleanseReport taskRunCleanseReport,
    ILogger<CleanseReportJob> logger) : IJob
{
    public async Task Execute(IJobExecutionContext context)
    {
        logger.LogInformation("CleanseReportJob started at {StartTime}", DateTime.UtcNow);

        try
        {
            await taskRunCleanseReport.RunAsync(context.CancellationToken);

            logger.LogInformation("CleanseReportJob completed at {EndTime}", DateTime.UtcNow);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "CleanseReportJob failed");
            throw;
        }
    }
}
