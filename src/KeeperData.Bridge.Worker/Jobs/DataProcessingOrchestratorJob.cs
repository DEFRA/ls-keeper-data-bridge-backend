using KeeperData.Bridge.Worker.Tasks;
using Quartz;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeeperData.Bridge.Worker.Jobs
{
    //Prevents Quartz from starting a new job instance if the previous one is still running
    [DisallowConcurrentExecution]
    public class DataProcessingOrchestratorJob(
        ILogger<DataProcessingOrchestratorJob> logger,
        ITaskDownload taskDownload,
        ITaskProcess taskProcess) : IJob
    {
        public async Task Execute(IJobExecutionContext context)
        {
            logger.LogInformation("Orchestration Job started at {startTime}", DateTime.UtcNow);

            try
            {
                await taskDownload.RunAsync(context.CancellationToken);
                await taskProcess.RunAsync(context.CancellationToken);

                logger.LogInformation("Orchestration Job finished successfully at {endTime}", DateTime.UtcNow);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Orchestration Job failed.");
                // Optionally, rethrow the exception to have Quartz mark the job as failed
                throw;
            }
        }
    }
}
