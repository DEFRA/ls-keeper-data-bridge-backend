using KeeperData.Core.Locking;
using KeeperData.Core.Reports.Cleanse.Export.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Export.Command.AggregateRoots;
using KeeperData.Core.Reports.Cleanse.Export.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Export.Operations.Abstract;
using KeeperData.Core.Reports.Cleanse.Export.Operations.Dtos;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Cleanse.Export.Command;

/// <summary>
/// Orchestrates ad-hoc full cleanse report exports.
/// Runs the export in a long-running background task with a distributed lock.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Orchestration service with complex dependencies. Covered by integration and component tests.")]
public class CleanseExportCommandService(
    ICleanseReportExportCommandService reportExportService,
    ICleanseExportOperationRepository repository,
    ICleanseExportOperationQueries queries,
    IDistributedLock distributedLock,
    IBlobStorageServiceFactory blobStorageServiceFactory,
    ILogger<CleanseExportCommandService> logger) : ICleanseExportCommandService
{
    private const string LockName = "cleanse-full-export";
    private static readonly TimeSpan LockDuration = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan LockRenewalInterval = TimeSpan.FromMinutes(2);

    /// <inheritdoc />
    public async Task<CleanseExportOperationDto?> StartFullExportAsync(CancellationToken ct = default)
    {
        var lockHandle = await distributedLock.TryAcquireAsync(LockName, LockDuration, ct);
        if (lockHandle is null)
            return null;

        var operation = CleanseExportOperation.Create();
        await repository.CreateAsync(operation, ct);
        var dto = await queries.GetOperationAsync(operation.Id, ct);

        _ = Task.Factory.StartNew(
            async () =>
            {
                try
                {
                    await RunExportWithLockAsync(operation, lockHandle, CancellationToken.None);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Background full export failed (exportId={ExportId})", operation.Id);
                }
            },
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default
        ).Unwrap();

        return dto;
    }

    /// <inheritdoc />
    public async Task<CleanseExportOperationDto?> GetExportOperationAsync(string exportId, CancellationToken ct = default)
    {
        return await queries.GetOperationAsync(exportId, ct);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<CleanseExportOperationSummaryDto>> GetExportOperationsAsync(
        int skip, int top, CancellationToken ct = default)
    {
        return await queries.GetOperationsAsync(skip, top, ct);
    }

    /// <inheritdoc />
    public async Task<RegenerateReportUrlResult> RegenerateExportUrlAsync(string exportId, CancellationToken ct = default)
    {
        try
        {
            var operation = await repository.GetByIdAsync(exportId, ct);
            if (operation is null)
            {
                return new RegenerateReportUrlResult
                {
                    Success = false,
                    OperationId = exportId,
                    Error = $"Export operation not found: {exportId}"
                };
            }

            if (string.IsNullOrEmpty(operation.ReportObjectKey))
            {
                return new RegenerateReportUrlResult
                {
                    Success = false,
                    OperationId = exportId,
                    Error = "Export operation does not have a report file. The export may not have completed successfully."
                };
            }

            var blobService = blobStorageServiceFactory.GetCleanseReportsBlobService();
            var newUrl = blobService.GeneratePresignedUrl(operation.ReportObjectKey);

            operation.UpdateReportUrl(newUrl);
            await repository.UpdateAsync(operation, ct);

            logger.LogInformation("Regenerated presigned URL for export {ExportId}. New URL: {ReportUrl}", exportId, newUrl);

            return new RegenerateReportUrlResult
            {
                Success = true,
                OperationId = exportId,
                ObjectKey = operation.ReportObjectKey,
                ReportUrl = newUrl
            };
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to regenerate presigned URL for export {ExportId}", exportId);
            return new RegenerateReportUrlResult
            {
                Success = false,
                OperationId = exportId,
                Error = ex.Message
            };
        }
    }

    private async Task RunExportWithLockAsync(CleanseExportOperation operation, IDistributedLockHandle lockHandle, CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        using var renewalCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var renewalTask = StartLockRenewalAsync(lockHandle, renewalCts.Token);

        try
        {
            operation.Start();
            await repository.UpdateAsync(operation, ct);

            var options = new ExportOptions { Since = null, SendNotification = false };

            var result = await reportExportService.ExportToStorageAsync(
                options,
                async (recordsProcessed, totalRecords, stepDescription) =>
                {
                    operation.UpdateProgress(recordsProcessed, totalRecords, stepDescription);
                    await repository.UpdateAsync(operation, ct);
                },
                ct);

            stopwatch.Stop();

            if (result.Success && !string.IsNullOrEmpty(result.ReportUrl) && !string.IsNullOrEmpty(result.ObjectKey))
            {
                operation.SetReportDetails(result.ObjectKey, result.ReportUrl);
                operation.Complete(stopwatch.ElapsedMilliseconds);
            }
            else
            {
                operation.Fail(result.Error ?? "Export produced no output", stopwatch.ElapsedMilliseconds);
            }

            await repository.UpdateAsync(operation, ct);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            operation.Fail(ex.Message, stopwatch.ElapsedMilliseconds);
            await repository.UpdateAsync(operation, CancellationToken.None);
        }
        finally
        {
            await renewalCts.CancelAsync();
            try { await renewalTask; } catch { /* Ignore cancellation */ }
            await lockHandle.DisposeAsync();
        }
    }

    private static async Task StartLockRenewalAsync(IDistributedLockHandle lockHandle, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await Task.Delay(LockRenewalInterval, ct);
            if (!await lockHandle.TryRenewAsync(LockDuration, ct))
                break;
        }
    }
}
