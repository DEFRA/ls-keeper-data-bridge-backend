using System.Diagnostics.CodeAnalysis;
using KeeperData.Bridge.Worker.Coordination;
using KeeperData.Infrastructure.Storage;
using Microsoft.Extensions.Logging;
using Quartz;

namespace KeeperData.Bridge.Worker.Jobs;

/// <summary>
/// Daily trigger for the ETL pipeline, equivalent to an unfiltered POST to <c>api/etl/imports</c>.
/// The coordinator only queues the run, so this job returns as soon as the run is accepted.
/// </summary>
[DisallowConcurrentExecution]
[ExcludeFromCodeCoverage(Justification = "Quartz job wrapper - covered by integration tests.")]
public class EtlImportJob(
    IEtlImportCoordinator coordinator,
    ILogger<EtlImportJob> logger) : IJob
{
    public async Task Execute(IJobExecutionContext context)
    {
        try
        {
            // Not context.CancellationToken: the run outlives this job execution, and Quartz
            // disposes the execution context (and its token source) as soon as Execute returns.
            // Shutdown is already handled by the runner, which links ApplicationStopping.
            var result = await coordinator.StartAsync(BlobStorageSources.External, dataset: null, CancellationToken.None);

            if (result.Accepted)
            {
                logger.LogInformation("EtlImportJob started an ETL import (importId={importId})", result.ImportId);
            }
            else
            {
                logger.LogWarning(
                    "EtlImportJob skipped, an ETL import is already running (inFlightImportId={inFlightImportId})",
                    result.InFlightImportId);
            }
        }
        catch (Exception ex)
        {
            // Never let the job throw: log it and let the next scheduled run retry.
            logger.LogError(ex, "EtlImportJob failed to start an ETL import");
        }
    }
}
