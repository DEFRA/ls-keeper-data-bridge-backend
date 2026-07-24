using KeeperData.Bridge.Worker.Tasks;
using Microsoft.Extensions.Logging;
using Quartz;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Worker.Jobs;

[DisallowConcurrentExecution]
[ExcludeFromCodeCoverage(Justification = "Quartz job wrapper - covered by task/service tests.")]
public class RotateExternalStorageKeysJob(
    ITaskRotateExternalStorageKeys taskRotateExternalStorageKeys,
    ILogger<RotateExternalStorageKeysJob> logger) : IJob
{
    public async Task Execute(IJobExecutionContext context)
    {
        try
        {
            await taskRotateExternalStorageKeys.RunAsync(context.CancellationToken);
        }
        catch (Exception ex)
        {
            // Never let the job throw: log with the identifiable prefix and let the next run retry.
            logger.LogError(ex, "[KeyRotation] RotateExternalStorageKeysJob failed");
        }
    }
}
