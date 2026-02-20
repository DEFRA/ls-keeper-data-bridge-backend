using KeeperData.Core.Reports;
using Microsoft.Extensions.Logging;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Worker.Tasks.Implementations;

[ExcludeFromCodeCoverage(Justification = "Background task with service dependency - covered by integration tests.")]
public class TaskRunCleanseReport(ILogger<TaskRunCleanseReport> logger, ICleanseFacade cleanseFacade) : ITaskRunCleanseReport
{
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Starting scheduled cleanse report analysis");

        var operation = await cleanseFacade.Commands.CleanseAnalysisCommandService.StartAnalysisAsync(cancellationToken);

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
