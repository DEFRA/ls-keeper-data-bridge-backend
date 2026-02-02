using System.Diagnostics.CodeAnalysis;
using KeeperData.Bridge.Worker.Tasks;
using Microsoft.Extensions.Logging;
using Quartz;

namespace KeeperData.Bridge.Worker.Jobs;

[DisallowConcurrentExecution]
[ExcludeFromCodeCoverage(Justification = "Quartz job wrapper - covered by integration tests.")]
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
        catch (Exception)
        {
            // Rethrow without logging - Quartz will handle the exception logging
            throw;
        }
    }
}
