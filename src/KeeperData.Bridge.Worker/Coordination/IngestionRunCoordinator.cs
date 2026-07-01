using System.Diagnostics.CodeAnalysis;
using KeeperData.Bridge.Worker.Tasks;
using KeeperData.Core.Locking;
using KeeperData.Infrastructure.Storage;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KeeperData.Bridge.Worker.Coordination;

/// <summary>
/// Holds the run lock (previously owned by the legacy task) and dispatches the run.
/// Lock acquisition and renewal moved here so the coordinator is the single point of
/// mutual exclusion for ingestion.
/// </summary>
public sealed class IngestionRunCoordinator(
    ILogger<IngestionRunCoordinator> logger,
    IDistributedLock distributedLock,
    ITaskProcessBulkFiles legacyImport,
    IHostApplicationLifetime applicationLifetime,
    IOptions<IngestionRunOptions> options) : IIngestionRunCoordinator
{
    private readonly IngestionRunOptions _options = options.Value;

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var runId = Guid.NewGuid();
        logger.LogInformation("Attempting to acquire lock for {LockName} (runId={runId}).", _options.LockName, runId);

        await using var @lock = await distributedLock.TryAcquireAsync(_options.LockName, _options.LockDuration, cancellationToken);
        if (@lock is null)
        {
            logger.LogInformation("Could not acquire lock for {LockName}, another run is likely in progress (runId={runId}).", _options.LockName, runId);
            return;
        }

        logger.LogInformation("Lock acquired for {LockName}. Run started at {startTime} (runId={runId}).", _options.LockName, DateTime.UtcNow, runId);
        await ExecuteWithRenewalAsync(@lock, runId, BlobStorageSources.External, cancellationToken);
    }

    public async Task<Guid?> StartAsync(string sourceType, CancellationToken cancellationToken = default)
    {
        var runId = Guid.NewGuid();
        logger.LogInformation("Attempting to acquire lock for {LockName} with sourceType={sourceType} (runId={runId}).", _options.LockName, sourceType, runId);

        var @lock = await distributedLock.TryAcquireAsync(_options.LockName, _options.LockDuration, cancellationToken);
        if (@lock is null)
        {
            logger.LogInformation("Could not acquire lock for {LockName}, another run is likely in progress (runId={runId}).", _options.LockName, runId);
            return null;
        }

        logger.LogInformation("Lock acquired for {LockName}. Starting run in background with sourceType={sourceType} (runId={runId}).", _options.LockName, sourceType, runId);

        StartRunInBackground(@lock, runId, sourceType, cancellationToken);

        return runId;
    }

    // Fire-and-forget dispatch onto a background thread. Excluded from coverage because it is
    // timing/threading bound and not unit-testable without real delays; exercised by integration tests.
    [ExcludeFromCodeCoverage(Justification = "Background dispatch - exercised by integration tests, not unit-testable without real delays.")]
    private void StartRunInBackground(IDistributedLockHandle @lock, Guid runId, string sourceType, CancellationToken cancellationToken)
    {
        var stoppingToken = applicationLifetime.ApplicationStopping;

        _ = Task.Factory.StartNew(
            async () =>
            {
                try
                {
                    await using (@lock)
                    {
                        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, stoppingToken);
                        await ExecuteWithRenewalAsync(@lock, runId, sourceType, cts.Token);
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

    private async Task ExecuteWithRenewalAsync(IDistributedLockHandle lockHandle, Guid runId, string sourceType, CancellationToken externalCancellationToken)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(externalCancellationToken);
        var renewalTask = RenewLockPeriodicallyAsync(lockHandle, linkedCts.Token, runId);

        try
        {
            await legacyImport.RunImportAsync(runId, sourceType, linkedCts.Token);
        }
        catch (OperationCanceledException) when (externalCancellationToken.IsCancellationRequested)
        {
            logger.LogInformation("Run was cancelled at {endTime}, (runId={runId})", DateTime.UtcNow, runId);
            throw;
        }
        catch (OperationCanceledException) when (linkedCts.IsCancellationRequested && !externalCancellationToken.IsCancellationRequested)
        {
            logger.LogError("Run was stopped due to lock renewal failure at {endTime}, (runId={runId})", DateTime.UtcNow, runId);
            throw new InvalidOperationException("Run was cancelled due to lock renewal failure");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error occurred during run execution (runId={runId})", runId);
            throw;
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
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Unexpected error in lock renewal task for {LockName} (runId={runId})", _options.LockName, runId);
            }
        }
    }

    // Timer-driven lock renewal. Excluded from coverage because it depends on real delays and
    // cannot be unit-tested deterministically without a time abstraction; exercised by integration tests.
    [ExcludeFromCodeCoverage(Justification = "Timer-driven renewal loop - not unit-testable without real delays.")]
    private async Task RenewLockPeriodicallyAsync(IDistributedLockHandle lockHandle, CancellationToken cancellationToken, Guid runId)
    {
        logger.LogDebug("Starting lock renewal task for {LockName} with interval {RenewalInterval} (runId={runId})", _options.LockName, _options.RenewalInterval, runId);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.RenewalInterval, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                logger.LogDebug("Lock renewal task cancelled for {LockName} (runId={runId})", _options.LockName, runId);
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
                logger.LogDebug("Lock renewal cancelled for {LockName} (runId={runId})", _options.LockName, runId);
                return;
            }

            if (renewed)
            {
                logger.LogDebug("Successfully renewed lock for {LockName} with extension {RenewalExtension} (runId={runId})", _options.LockName, _options.RenewalExtension, runId);
            }
            else
            {
                logger.LogError("Failed to renew lock for {LockName}. Lock may have been lost. Cancelling run. (runId={runId})", _options.LockName, runId);
                throw new InvalidOperationException($"Failed to renew lock for {_options.LockName} (runId={runId})");
            }
        }
    }
}
