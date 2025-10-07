using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Locking;
using KeeperData.Infrastructure.Storage;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KeeperData.Bridge.Worker.Tasks.Implementations;

public class TaskProcessBulkFiles(
    ILogger<TaskProcessBulkFiles> logger, 
    IDistributedLock distributedLock, 
    IImportPipeline importPipeline,
    IHostApplicationLifetime applicationLifetime) : ITaskProcessBulkFiles
{
    private const string LockName = nameof(TaskProcessBulkFiles);
    private static readonly TimeSpan LockDuration = TimeSpan.FromMinutes(4);       
    private static readonly TimeSpan RenewalInterval = TimeSpan.FromMinutes(1);      
    private static readonly TimeSpan RenewalExtension = TimeSpan.FromMinutes(2);   
    
    public async Task<Guid?> StartAsync(CancellationToken cancellationToken = default)
    {
        var importId = Guid.NewGuid();
        
        logger.LogInformation("Attempting to acquire lock for {LockName} (importid={importId}).", LockName, importId);

        var @lock = await distributedLock.TryAcquireAsync(LockName, LockDuration, cancellationToken);

        if (@lock == null)
        {
            logger.LogInformation("Could not acquire lock for {LockName}, another instance is likely running (importid={importId}).", LockName, importId);
            return null;
        }

        logger.LogInformation("Lock acquired for {LockName}. Starting import in background (importid={importId}).", LockName, importId);
        
        var stoppingToken = applicationLifetime.ApplicationStopping;
        
        _ = Task.Factory.StartNew(
            async () => 
            {
                try
                {
                    await using (@lock)
                    {
                        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
                            cancellationToken, 
                            stoppingToken);
                        
                        await ExecuteImportWithLockRenewalAsync(@lock, importId, cts.Token);
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    logger.LogWarning("Application is shutting down, import cancelled (importid={importId})", importId);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Background import failed (importid={importId})", importId);
                }
            },
            CancellationToken.None,
            TaskCreationOptions.LongRunning,    
            TaskScheduler.Default
        ).Unwrap();
        
        return importId;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var importId = Guid.NewGuid();

        logger.LogInformation("Attempting to acquire lock for {LockName} (importid={importId}).", LockName, importId);

        await using var @lock = await distributedLock.TryAcquireAsync(LockName, LockDuration, cancellationToken);

        if (@lock == null)
        {
            logger.LogInformation("Could not acquire lock for {LockName}, another instance is likely running  (importid={importId}).", LockName, importId);
            return;
        }

        logger.LogInformation("Lock acquired for {LockName}. Task started at {startTime} (importid={importId}).", LockName, DateTime.UtcNow, importId);
        
        await ExecuteImportWithLockRenewalAsync(@lock, importId, cancellationToken);
    }

    private async Task ExecuteImportWithLockRenewalAsync(
        IDistributedLockHandle lockHandle, 
        Guid importId, 
        CancellationToken externalCancellationToken)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(externalCancellationToken);
        
        var renewalTask = RenewLockPeriodicallyAsync(lockHandle, linkedCts.Token, importId);
        
        try
        {
            await importPipeline.StartAsync(importId, BlobStorageSources.External, linkedCts.Token);
            
            logger.LogInformation("Import completed successfully at {endTime}, (importid={importId})", DateTime.UtcNow, importId);
        }
        catch (OperationCanceledException) when (externalCancellationToken.IsCancellationRequested)
        {
            logger.LogInformation("Import was cancelled at {endTime}, (importid={importId})", DateTime.UtcNow, importId);
            throw;
        }
        catch (OperationCanceledException) when (linkedCts.IsCancellationRequested && !externalCancellationToken.IsCancellationRequested)
        {
            logger.LogError("Import was stopped due to lock renewal failure at {endTime}, (importid={importId})", DateTime.UtcNow, importId);
            throw new InvalidOperationException("Task was cancelled due to lock renewal failure");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error occurred during import execution (importid={importId})", importId);
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
                logger.LogError(ex, "Unexpected error in lock renewal task for {LockName} (importid={importId})", LockName, importId);
            }
        }
    }

    private async Task RenewLockPeriodicallyAsync(IDistributedLockHandle lockHandle, CancellationToken cancellationToken, Guid importId)
    {
        logger.LogDebug("Starting lock renewal task for {LockName} with interval {RenewalInterval} (importid={importId})", LockName, RenewalInterval, importId);
        
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(RenewalInterval, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                logger.LogDebug("Lock renewal task cancelled for {LockName} (importid={importId})", LockName, importId);
                return;
            }
            
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            
            logger.LogDebug("Attempting to renew lock for {LockName} (importid={importId})", LockName, importId);
            
            bool renewed;
            try
            {
                renewed = await lockHandle.TryRenewAsync(RenewalExtension, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                logger.LogDebug("Lock renewal cancelled for {LockName} (importid={importId})", LockName, importId);
                return;
            }
            
            if (renewed)
            {
                logger.LogDebug("Successfully renewed lock for {LockName} with extension {RenewalExtension} (importid={importId})", LockName, RenewalExtension, importId);
            }
            else
            {
                logger.LogError("Failed to renew lock for {LockName}. Lock may have been lost. Cancelling main task. (importid={importId})", LockName, importId);
                
                throw new InvalidOperationException($"Failed to renew lock for {LockName} (importid={importId})");
            }
        }
        
        logger.LogDebug("Lock renewal task completed for {LockName} (importid={importId})", LockName, importId);
    }
}