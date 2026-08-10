using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Locking;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KeeperData.Bridge.Worker.Coordination;

/// <summary>How often to renew a held lock, and by how much.</summary>
public sealed record LockRenewalSettings(string LockName, TimeSpan RenewalInterval, TimeSpan RenewalExtension);

/// <summary>
/// Runs work while keeping an already-acquired lock alive, inline or on a background thread.
///
/// Shared by the legacy ingestion executor and the file-based import executor: the renewal loop is
/// the fiddliest code in this folder and having two copies of it would be a mistake.
/// </summary>
public interface ILockRenewingRunner
{
    /// <summary>Runs <paramref name="work"/> inline, renewing the lock until it completes.</summary>
    Task RunAsync(
        IDistributedLockHandle lockHandle,
        LockRenewalSettings settings,
        Guid runId,
        Func<CancellationToken, Task> work,
        CancellationToken cancellationToken);

    /// <summary>Runs <paramref name="work"/> on a background thread; takes ownership of disposing
    /// the lock handle. <paramref name="onFailure"/> is invoked for failures that happen outside the
    /// work itself (lock loss, shutdown), so a caller tracking status can record them.</summary>
    void StartInBackground(
        IDistributedLockHandle lockHandle,
        LockRenewalSettings settings,
        Guid runId,
        Func<CancellationToken, Task> work,
        Func<Exception, Task>? onFailure,
        CancellationToken cancellationToken);
}

[ExcludeFromCodeCoverage(Justification = "Timing/threading-bound lock renewal and background dispatch; exercised by integration tests.")]
public sealed class LockRenewingRunner(
    ILogger<LockRenewingRunner> logger,
    IHostApplicationLifetime applicationLifetime) : ILockRenewingRunner
{
    public void StartInBackground(
        IDistributedLockHandle lockHandle,
        LockRenewalSettings settings,
        Guid runId,
        Func<CancellationToken, Task> work,
        Func<Exception, Task>? onFailure,
        CancellationToken cancellationToken)
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
                        await RunAsync(lockHandle, settings, runId, work, cts.Token);
                    }
                }
                catch (OperationCanceledException cancelled) when (stoppingToken.IsCancellationRequested)
                {
                    logger.LogWarning("Application is shutting down, run cancelled (runId={runId})", runId);
                    await ReportAsync(onFailure, cancelled, runId);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Background run failed (runId={runId})", runId);
                    await ReportAsync(onFailure, ex, runId);
                }
            },
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default
        ).Unwrap();
    }

    public async Task RunAsync(
        IDistributedLockHandle lockHandle,
        LockRenewalSettings settings,
        Guid runId,
        Func<CancellationToken, Task> work,
        CancellationToken cancellationToken)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var renewalTask = RenewLockPeriodicallyAsync(lockHandle, settings, linkedCts.Token, runId);

        try
        {
            await work(linkedCts.Token);
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
                logger.LogError(ex, "Unexpected error in lock renewal task for {LockName} (runId={runId})", settings.LockName, runId);
            }
        }
    }

    private async Task ReportAsync(Func<Exception, Task>? onFailure, Exception exception, Guid runId)
    {
        if (onFailure is null) return;

        try
        {
            await onFailure(exception);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failure callback threw and was ignored (runId={runId})", runId);
        }
    }

    private async Task RenewLockPeriodicallyAsync(
        IDistributedLockHandle lockHandle,
        LockRenewalSettings settings,
        CancellationToken cancellationToken,
        Guid runId)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(settings.RenewalInterval, cancellationToken);
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
                renewed = await lockHandle.TryRenewAsync(settings.RenewalExtension, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            if (!renewed)
            {
                logger.LogError("Failed to renew lock for {LockName}. Lock may have been lost. Cancelling run. (runId={runId})", settings.LockName, runId);
                throw new InvalidOperationException($"Failed to renew lock for {settings.LockName} (runId={runId})");
            }
        }
    }
}
