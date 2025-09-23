using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeeperData.Bridge.Worker.Tasks
{
    public class TaskProcess(ILogger<TaskProcess> logger) : ITaskProcess
    {
        public async Task RunAsync(CancellationToken cancellationToken)
        {
            logger.LogInformation("TASK Process: Starting data processing...");
            await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken); 
            logger.LogInformation("TASK Process: Data processing complete.");
        }
    }
}
