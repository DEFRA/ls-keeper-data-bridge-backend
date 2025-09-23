using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeeperData.Bridge.Worker.Tasks
{
    public class TaskDownload(ILogger<TaskDownload> logger) : ITaskDownload
    {
        public async Task RunAsync(CancellationToken cancellationToken)
        {
            logger.LogInformation("TASK Download: Starting data download...");
            await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            logger.LogInformation("TASK Download: Data download complete.");
        }
    }
}
