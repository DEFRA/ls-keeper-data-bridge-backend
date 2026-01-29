using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Locking;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Reports.Abstract;
using KeeperData.Core.Reports.Analysis;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Dtos;
using KeeperData.Core.Reports.Strategies;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.Reports;

/// <summary>
/// Service for running cleanse analysis and managing cleanse report data.
/// Orchestrates analysis by delegating to registered strategies.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Orchestration service with complex dependencies. Covered by integration and component tests.")]
public class CleanseReportService(
    IQueryService queryService,
    DataSetDefinitions dataSets,
    ICleanseReportRepository reportRepository,
    ICleanseAnalysisRepository analysisRepository,
    IDistributedLock distributedLock,
    ICleanseReportExportService exportService,
    ICleanseReportPresignedUrlGenerator presignedUrlGenerator,
    ICleanseReportNotificationService notificationService,
    ILogger<CleanseReportService> logger,
    IEnumerable<ICleanseAnalysisStrategy> strategies) : ICleanseReportService
{
    private const string LockName = "cleanse-analysis";
    private static readonly TimeSpan LockDuration = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan LockRenewalInterval = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Starts a cleanse analysis operation in the background using a long-running thread.
    /// Returns immediately after acquiring the lock and starting the background task.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The operation if started, or null if the lock could not be acquired.</returns>
    public async Task<CleanseAnalysisOperation?> StartAnalysisAsync(CancellationToken ct = default)
    {
        var lockHandle = await AcquireLockAsync(ct);
        if (lockHandle is null)
            return null;

        var operation = await CreateOperationAsync(ct);

        // Use a long-running thread to avoid thread pool starvation
        _ = Task.Factory.StartNew(
            async () =>
            {
                try
                {
                    await RunAnalysisWithLockAsync(operation, lockHandle, ct);
                }
                catch (OperationCanceledException)
                {
                    logger.LogWarning("Cleanse analysis was cancelled (operationId={OperationId})", operation.Id);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Background cleanse analysis failed (operationId={OperationId})", operation.Id);
                }
            },
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default
        ).Unwrap();

        return operation;
    }

    /// <summary>
    /// Runs a cleanse analysis operation synchronously on the caller thread.
    /// Exceptions are propagated to the caller. Useful for testing.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The completed operation, or null if the lock could not be acquired.</returns>
    public async Task<CleanseAnalysisOperation?> RunAnalysisAsync(CancellationToken ct = default)
    {
        var lockHandle = await AcquireLockAsync(ct);
        if (lockHandle is null)
            return null;

        var operation = await CreateOperationAsync(ct);
        await RunAnalysisWithLockAsync(operation, lockHandle, ct);
        return await GetOperationAsync(operation.Id, ct);
    }

    public Task<CleanseAnalysisOperation?> GetOperationAsync(string operationId, CancellationToken ct = default)
        => analysisRepository.GetOperationAsync(operationId, ct);

    public Task<IReadOnlyList<CleanseAnalysisOperationSummary>> GetOperationsAsync(int skip = 0, int top = 10, CancellationToken ct = default)
        => analysisRepository.GetOperationsAsync(skip, top, ct);

    public Task<CleanseAnalysisOperation?> GetCurrentOperationAsync(CancellationToken ct = default)
        => analysisRepository.GetCurrentOperationAsync(ct);

    public async Task<CleanseIssuesResult> ListIssuesAsync(int skip = 0, int top = 50, CancellationToken ct = default)
    {
        var itemsTask = reportRepository.GetActiveIssuesAsync(skip, top, ct);
        var countTask = reportRepository.GetActiveIssuesCountAsync(ct);

        await Task.WhenAll(itemsTask, countTask);

        return new CleanseIssuesResult
        {
            Items = itemsTask.Result,
            TotalCount = (int)countTask.Result,
            Skip = skip,
            Top = top
        };
    }

    public async Task<CleanseDeleteResult> DeleteReportDataAsync(CancellationToken ct = default)
    {
        const string collectionName = "cleanse_report";

        try
        {
            var deletedCount = await reportRepository.DeleteAllAsync(ct);
            return new CleanseDeleteResult
            {
                Success = true,
                CollectionName = collectionName,
                Message = $"Successfully deleted {deletedCount} cleanse report items.",
                DeletedCount = deletedCount,
                OperatedAtUtc = DateTime.UtcNow
            };
        }
        catch (Exception ex)
        {
            return new CleanseDeleteResult
            {
                Success = false,
                CollectionName = collectionName,
                Message = $"Failed to delete cleanse report data: {ex.Message}",
                OperatedAtUtc = DateTime.UtcNow,
                Error = ex
            };
        }
    }

    public async Task<CleanseDeleteResult> DeleteMetadataAsync(CancellationToken ct = default)
    {
        const string collectionName = "cleanse_analysis_operations";

        try
        {
            var deletedCount = await analysisRepository.DeleteAllAsync(ct);
            return new CleanseDeleteResult
            {
                Success = true,
                CollectionName = collectionName,
                Message = $"Successfully deleted {deletedCount} analysis operation records.",
                DeletedCount = deletedCount,
                OperatedAtUtc = DateTime.UtcNow
            };
        }
        catch (Exception ex)
        {
            return new CleanseDeleteResult
            {
                Success = false,
                CollectionName = collectionName,
                Message = $"Failed to delete analysis metadata: {ex.Message}",
                OperatedAtUtc = DateTime.UtcNow,
                Error = ex
            };
        }
    }

    private async Task<IDistributedLockHandle?> AcquireLockAsync(CancellationToken ct)
        => await distributedLock.TryAcquireAsync(LockName, LockDuration, ct);

    private async Task<CleanseAnalysisOperation> CreateOperationAsync(CancellationToken ct)
    {
        var operation = new CleanseAnalysisOperation
        {
            Id = Guid.NewGuid().ToString(),
            Status = CleanseAnalysisStatus.Running,
            StartedAtUtc = DateTime.UtcNow,
            StatusDescription = "Initializing analysis..."
        };
        await analysisRepository.CreateOperationAsync(operation, ct);
        return operation;
    }

    private async Task RunAnalysisWithLockAsync(
        CleanseAnalysisOperation operation,
        IDistributedLockHandle lockHandle,
        CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        using var renewalCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var renewalTask = StartLockRenewalAsync(lockHandle, renewalCts.Token);

        try
        {
            // Create scoped context with cache for this analysis session
            var context = new AnalysisContext(operation.Id, queryService, dataSets);
            var issueRecorder = new IssueRecorder(reportRepository);

            var aggregateMetrics = new AggregateMetrics();

            foreach (var strategy in strategies)
            {
                await analysisRepository.UpdateProgressAsync(
                    operation.Id,
                    progressPercentage: 0,
                    statusDescription: $"Running strategy: {strategy.Name}",
                    recordsAnalyzed: aggregateMetrics.RecordsAnalyzed,
                    issuesFound: aggregateMetrics.IssuesFound,
                    issuesResolved: aggregateMetrics.IssuesResolved,
                    ct);

                var metrics = await strategy.ExecuteAsync(
                    context,
                    issueRecorder,
                    async (recordsAnalyzed, totalRecords, issuesFound, issuesResolved) =>
                    {
                        var percentage = totalRecords > 0 ? (double)recordsAnalyzed / totalRecords * 100 : 0;
                        await analysisRepository.UpdateProgressAsync(
                            operation.Id,
                            percentage,
                            $"[{strategy.Name}] Analyzed {recordsAnalyzed} of {totalRecords} records",
                            aggregateMetrics.RecordsAnalyzed + recordsAnalyzed,
                            aggregateMetrics.IssuesFound + issuesFound,
                            aggregateMetrics.IssuesResolved + issuesResolved,
                            ct);
                    },
                    ct);

                aggregateMetrics.RecordsAnalyzed += metrics.RecordsAnalyzed;
                aggregateMetrics.IssuesFound += metrics.IssuesFound;
                aggregateMetrics.IssuesResolved += metrics.IssuesResolved;
            }

            stopwatch.Stop();
            await CompleteOperationAsync(operation.Id, aggregateMetrics, stopwatch.ElapsedMilliseconds, ct);

            // Export report to CSV, zip, and upload to S3
            await ExportReportAsync(operation.Id, ct);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await FailOperationAsync(operation.Id, ex.Message, stopwatch.ElapsedMilliseconds, ct);
        }
        finally
        {
            renewalCts.Cancel();
            try { await renewalTask; } catch { /* Ignore cancellation */ }
            await lockHandle.DisposeAsync();
        }
    }

    private async Task StartLockRenewalAsync(IDistributedLockHandle lockHandle, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await Task.Delay(LockRenewalInterval, ct);
            if (!await lockHandle.TryRenewAsync(LockDuration, ct))
                break;
        }
    }

    private async Task CompleteOperationAsync(string operationId, AggregateMetrics metrics, long durationMs, CancellationToken ct)
    {
        await analysisRepository.CompleteOperationAsync(
            operationId,
            metrics.RecordsAnalyzed,
            metrics.IssuesFound,
            metrics.IssuesResolved,
            durationMs,
            ct);
    }

    private async Task ExportReportAsync(string operationId, CancellationToken ct)
    {
        try
        {
            logger.LogInformation("Starting report export for operation {OperationId}", operationId);

            var exportResult = await exportService.ExportAndUploadAsync(operationId, ct);

            if (exportResult.Success && !string.IsNullOrEmpty(exportResult.ReportUrl) && !string.IsNullOrEmpty(exportResult.ObjectKey))
            {
                await analysisRepository.SetReportDetailsAsync(operationId, exportResult.ObjectKey, exportResult.ReportUrl, ct);
                logger.LogInformation(
                    "Cleanse report exported successfully for operation {OperationId}. Report URL: {ReportUrl}",
                    operationId, exportResult.ReportUrl);

                // Send email notification with the report URL
                await SendNotificationAsync(operationId, exportResult.ReportUrl, ct);
            }
            else
            {
                logger.LogWarning(
                    "Failed to export cleanse report for operation {OperationId}: {Error}",
                    operationId, exportResult.Error ?? "Unknown error");
            }
        }
        catch (Exception ex)
        {
            // Log but don't fail the operation - the analysis completed successfully
            logger.LogError(ex, "Exception during report export for operation {OperationId}", operationId);
        }
    }

    private async Task SendNotificationAsync(string operationId, string reportUrl, CancellationToken ct)
    {
        try
        {
            logger.LogInformation("Sending cleanse report notification for operation {OperationId}", operationId);

            var notificationResult = await notificationService.SendReportNotificationAsync(reportUrl, ct);

            if (notificationResult.Success)
            {
                logger.LogInformation(
                    "Cleanse report notification sent successfully for operation {OperationId}. NotificationId: {NotificationId}, Recipient: {Recipient}",
                    operationId, notificationResult.NotificationId, notificationResult.Recipient);
            }
            else
            {
                logger.LogWarning(
                    "Failed to send cleanse report notification for operation {OperationId}: {Error}",
                    operationId, notificationResult.Error ?? "Unknown error");
            }
        }
        catch (Exception ex)
        {
            // Log but don't fail - notification is non-critical
            logger.LogError(ex, "Exception during notification send for operation {OperationId}", operationId);
        }
    }

    public async Task<RegenerateReportUrlResult> RegenerateReportUrlAsync(string operationId, CancellationToken ct = default)
    {
        try
        {
            var operation = await analysisRepository.GetOperationAsync(operationId, ct);

            if (operation is null)
            {
                return new RegenerateReportUrlResult
                {
                    Success = false,
                    OperationId = operationId,
                    Error = $"Operation not found: {operationId}"
                };
            }

            if (string.IsNullOrEmpty(operation.ReportObjectKey))
            {
                return new RegenerateReportUrlResult
                {
                    Success = false,
                    OperationId = operationId,
                    Error = "Operation does not have a report file. The analysis may not have completed successfully."
                };
            }

            var newUrl = presignedUrlGenerator.GeneratePresignedUrl(operation.ReportObjectKey);

            await analysisRepository.UpdateReportUrlAsync(operationId, newUrl, ct);

            logger.LogInformation(
                "Regenerated presigned URL for operation {OperationId}. New Report URL: {ReportUrl}",
                operationId, newUrl);

            return new RegenerateReportUrlResult
            {
                Success = true,
                OperationId = operationId,
                ObjectKey = operation.ReportObjectKey,
                ReportUrl = newUrl
            };
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to regenerate presigned URL for operation {OperationId}", operationId);
            return new RegenerateReportUrlResult
            {
                Success = false,
                OperationId = operationId,
                Error = ex.Message
            };
        }
    }

    private async Task FailOperationAsync(string operationId, string error, long durationMs, CancellationToken ct)
    {
        await analysisRepository.FailOperationAsync(operationId, error, durationMs, ct);
    }

    private sealed class AggregateMetrics
    {
        public int RecordsAnalyzed { get; set; }
        public int IssuesFound { get; set; }
        public int IssuesResolved { get; set; }
    }
}
