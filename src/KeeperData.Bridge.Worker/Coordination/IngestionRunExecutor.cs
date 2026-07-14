using System.Diagnostics.CodeAnalysis;
using KeeperData.Bridge.Worker.Tasks;
using KeeperData.Core.Pipeline;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core;
using KeeperData.Core.Locking;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KeeperData.Bridge.Worker.Coordination;

/// <summary>
/// Owns lock renewal and background dispatch for a run. Excluded from coverage because this code
/// is timing/threading-bound (real delays, fire-and-forget) and is exercised by integration tests;
/// the coordinator that decides whether a run happens is unit-tested.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Timing/threading-bound lock renewal and background dispatch; exercised by integration tests.")]
public sealed class IngestionRunExecutor(
    ILogger<IngestionRunExecutor> logger,
    ITaskProcessBulkFiles legacyImport,
    IEtlPipelineFactory etlPipelineFactory,
    IPipelineExecutor pipelineExecutor,
    IHostApplicationLifetime applicationLifetime,
    IOptions<IngestionRunOptions> options) : IIngestionRunExecutor
{
    private readonly IngestionRunOptions _options = options.Value;
    private readonly bool _runNewPipeline = false; // keep this off , until we're ready
        
    public void StartInBackground(IDistributedLockHandle lockHandle, Guid runId, string sourceType, CancellationToken cancellationToken)
    {
        var stoppingToken = applicationLifetime.ApplicationStopping;

        _ = Task.Factory.StartNew(
            async () =>
            {
                try
                {
                    await using (lockHandle)
                    {
                        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, stoppingToken);
                        await RunWithRenewalAsync(lockHandle, runId, sourceType, cts.Token);
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    logger.LogWarning("Application is shutting down, run cancelled (runId={runId})", runId);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Background run failed (runId={runId})", runId);
                }
            },
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default
        ).Unwrap();
    }

    public async Task RunWithRenewalAsync(IDistributedLockHandle lockHandle, Guid runId, string sourceType, CancellationToken cancellationToken)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var renewalTask = RenewLockPeriodicallyAsync(lockHandle, linkedCts.Token, runId);

        try
        {
            await legacyImport.RunImportAsync(runId, sourceType, linkedCts.Token);

            if (_runNewPipeline)
                await RunEtlPipelineAsync(runId, sourceType, linkedCts.Token);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            logger.LogInformation("Run was cancelled at {endTime}, (runId={runId})", DateTime.UtcNow, runId);
            throw;
        }
        catch (OperationCanceledException) when (linkedCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            logger.LogError("Run was stopped due to lock renewal failure at {endTime}, (runId={runId})", DateTime.UtcNow, runId);
            throw new InvalidOperationException("Run was cancelled due to lock renewal failure");
        }
        finally
        {
            if (!linkedCts.IsCancellationRequested)
            {
                await linkedCts.CancelAsync();
            }

            try
            {
                await renewalTask;
            }
            catch (OperationCanceledException)
            {
                // Expected: the renewal loop is cancelled once the run completes.
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Unexpected error in lock renewal task for {LockName} (runId={runId})", _options.LockName, runId);
            }
        }
    }

    private async Task RunEtlPipelineAsync(Guid runId, string sourceType, CancellationToken cancellationToken)
    {
        logger.LogInformation("ETL pipeline started (runId={runId})", runId);

        var pipeline = etlPipelineFactory.Create();
        var context = new EtlPipelineContext(runId, sourceType, EtlConstants.DefaultLookbackDays);

        await pipelineExecutor.RunAsync(pipeline, context, cancellationToken);

        logger.LogInformation("ETL pipeline completed (runId={runId})", runId);
    }

    private async Task RenewLockPeriodicallyAsync(IDistributedLockHandle lockHandle, CancellationToken cancellationToken, Guid runId)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.RenewalInterval, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            bool renewed;
            try
            {
                renewed = await lockHandle.TryRenewAsync(_options.RenewalExtension, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            if (!renewed)
            {
                logger.LogError("Failed to renew lock for {LockName}. Lock may have been lost. Cancelling run. (runId={runId})", _options.LockName, runId);
                throw new InvalidOperationException($"Failed to renew lock for {_options.LockName} (runId={runId})");
            }
        }
    }
}
