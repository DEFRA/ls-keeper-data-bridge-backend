using System.Diagnostics.CodeAnalysis;
using KeeperData.Bridge.Worker.Tasks;
using Microsoft.Extensions.Logging;
using Quartz;

namespace KeeperData.Bridge.Worker.Jobs;

[DisallowConcurrentExecution]
[ExcludeFromCodeCoverage(Justification = "Quartz job wrapper - covered by integration tests.")]
public class ImportDeltaFilesJob(
    ITaskProcessDeltaFiles taskProcessDeltaFiles,
    ILogger<ImportDeltaFilesJob> logger) : IJob
{
    public async Task Execute(IJobExecutionContext context)
    {
        logger.LogInformation("ImportDeltaFilesJob started at {startTime}", DateTime.UtcNow);

        try
        {
            await taskProcessDeltaFiles.RunAsync(context.CancellationToken);

            logger.LogInformation("ImportDeltaFilesJob completed at {endTime}", DateTime.UtcNow);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "ImportDeltaFilesJob failed.");
            throw;
        }
    }
}