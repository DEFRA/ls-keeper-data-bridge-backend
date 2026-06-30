using KeeperData.Core.ETL.Impl;
using Microsoft.Extensions.Logging;

namespace KeeperData.Bridge.Worker.Tasks.Implementations;

public class TaskProcessBulkFiles(
    ILogger<TaskProcessBulkFiles> logger,
    IImportOrchestrator importOrchestrator) : ITaskProcessBulkFiles
{
    public async Task RunImportAsync(Guid importId, string sourceType, CancellationToken cancellationToken)
    {
        await importOrchestrator.StartAsync(importId, sourceType, cancellationToken);

        logger.LogInformation("Import completed successfully at {endTime}, (importId={importId})", DateTime.UtcNow, importId);
    }
}
