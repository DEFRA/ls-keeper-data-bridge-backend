using KeeperData.Core.Reports.Abstract;
using Microsoft.Extensions.Logging;

namespace KeeperData.Bridge.Worker.Tasks.Implementations;

public class TaskRunCleanseReport(
    ILogger<TaskRunCleanseReport> logger,
    ICleanseReportService cleanseReportService) : ITaskRunCleanseReport
{
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Starting scheduled cleanse report analysis");

        var operation = await cleanseReportService.StartAnalysisAsync(cancellationToken);

        if (operation is null)
        {
            logger.LogWarning("Could not start cleanse report analysis - another analysis is already running");
            return;
        }

        logger.LogInformation(
            "Cleanse report analysis started with operation ID {OperationId}",
            operation.Id);
    }
}
