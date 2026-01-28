using System.Diagnostics.CodeAnalysis;
using KeeperData.Bridge.Worker.Tasks;
using Microsoft.Extensions.Logging;
using Quartz;

namespace KeeperData.Bridge.Worker.Jobs;

[DisallowConcurrentExecution]
[ExcludeFromCodeCoverage(Justification = "Quartz job wrapper - covered by integration tests.")]
public class ImportBulkFilesJob(
    ITaskProcessBulkFiles taskProcessBulkFiles,
    ILogger<ImportBulkFilesJob> logger) : IJob
{
    public async Task Execute(IJobExecutionContext context)
    {
        logger.LogInformation("ImportBulkFilesJob started at {startTime}", DateTime.UtcNow);

        try
        {
            await taskProcessBulkFiles.RunAsync(context.CancellationToken);

            logger.LogInformation("ImportBulkFilesJob completed at {endTime}", DateTime.UtcNow);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "ImportBulkFilesJob failed.");
            throw;
        }
    }
}