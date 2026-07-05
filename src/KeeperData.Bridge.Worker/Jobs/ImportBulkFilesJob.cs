using System.Diagnostics.CodeAnalysis;
using KeeperData.Bridge.Worker.Coordination;
using Microsoft.Extensions.Logging;
using Quartz;

namespace KeeperData.Bridge.Worker.Jobs;

[DisallowConcurrentExecution]
[ExcludeFromCodeCoverage(Justification = "Quartz job wrapper - covered by integration tests.")]
public class ImportBulkFilesJob(
    IIngestionRunCoordinator coordinator,
    ILogger<ImportBulkFilesJob> logger) : IJob
{
    public async Task Execute(IJobExecutionContext context)
    {
        logger.LogInformation("ImportBulkFilesJob started at {startTime}", DateTime.UtcNow);

        try
        {
            await coordinator.RunAsync(context.CancellationToken);

            logger.LogInformation("ImportBulkFilesJob completed at {endTime}", DateTime.UtcNow);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "ImportBulkFilesJob failed.");
            throw;
        }
    }
}
