using KeeperData.Core.Locking;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Export.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Command.Requests;
using KeeperData.Core.Reports.Cleanse.Operations.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Queries.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Queries.Dtos;
using KeeperData.Core.Reports.Issues.Command.Requests;
using KeeperData.Core.Reports.Issues.Command.Abstract;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Cleanse.Analysis.Command;

/// <summary>
/// Service for running cleanse analysis and managing cleanse report data.
/// Orchestrates analysis by delegating to registered strategies.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Orchestration service with complex dependencies. Covered by integration and component tests.")]
[SuppressMessage("SonarQube", "S107", Justification = "DI orchestration service requires multiple dependencies")]
public class CleanseAnalysisCommandService(
    ICleanseOperationCommandService operationCommandService,
    ICleanseAnalysisOperationsQueries operationQueries,
    IIssueCommandService issueCommandService,
    IDistributedLock distributedLock,
    ICleanseReportExportCommandService cleanseReportExportCommandService,
    ILogger<CleanseAnalysisCommandService> logger,
    ICleanseAnalysisEngine engine) : ICleanseAnalysisCommandService
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
    public async Task<CleanseAnalysisOperationDto?> StartAnalysisAsync(CancellationToken ct = default)
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
    public async Task<CleanseAnalysisOperationDto?> RunAnalysisAsync(CancellationToken ct = default)
    {
        var lockHandle = await AcquireLockAsync(ct);
        if (lockHandle is null)
            return null;

        var operation = await CreateOperationAsync(ct);
        await RunAnalysisWithLockAsync(operation, lockHandle, ct);
        return await operationQueries.GetOperationAsync(operation.Id, ct);
    }
    
    private async Task RunAnalysisWithLockAsync(CleanseAnalysisOperationDto operation, IDistributedLockHandle lockHandle, CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        using var renewalCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var renewalTask = StartLockRenewalAsync(lockHandle, renewalCts.Token);

        try
        {
            var aggregateMetrics = new AnalysisMetrics();

            await operationCommandService.UpdateProgressAsync(new UpdateProgressCommand(
                operation.Id,
                ProgressPercentage: 0,
                StatusDescription: "Running analysis",
                RecordsAnalyzed: aggregateMetrics.RecordsAnalyzed,
                IssuesFound: aggregateMetrics.IssuesFound,
                IssuesResolved: aggregateMetrics.IssuesResolved), ct);

            var metrics = await engine.ExecuteAsync(
                operation.Id,
                async (recordsAnalyzed, totalRecords, issuesFound, issuesResolved) =>
                {
                    var percentage = totalRecords > 0 ? (double)recordsAnalyzed / totalRecords * 100 : 0;
                    await operationCommandService.UpdateProgressAsync(new UpdateProgressCommand(
                        operation.Id,
                        percentage,
                        $"Analyzed {recordsAnalyzed} of {totalRecords} records",
                        aggregateMetrics.RecordsAnalyzed + recordsAnalyzed,
                        aggregateMetrics.IssuesFound + issuesFound,
                        aggregateMetrics.IssuesResolved + issuesResolved), ct);
                },
                ct);

            aggregateMetrics.RecordsAnalyzed += metrics.RecordsAnalyzed;
            aggregateMetrics.IssuesFound += metrics.IssuesFound;

            // Deactivate all issues not touched by this operation
            var deactivatedCount = await issueCommandService.DeactivateStaleIssuesAsync(
                new DeactivateStaleIssuesCommand(operation.Id), ct);
            aggregateMetrics.IssuesResolved += deactivatedCount;

            stopwatch.Stop();
            await operationCommandService.CompleteOperationAsync(new CompleteOperationCommand(
                operation.Id,
                metrics.RecordsAnalyzed,
                metrics.IssuesFound,
                metrics.IssuesResolved,
                stopwatch.ElapsedMilliseconds), ct);

            // Export report to CSV, zip, and upload to S3
            await cleanseReportExportCommandService.ExportReportAsync(operation.Id, ct);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await operationCommandService.FailOperationAsync(new FailOperationCommand(
                operation.Id,
                ex.Message,
                stopwatch.ElapsedMilliseconds), ct);
        }
        finally
        {
            await renewalCts.CancelAsync();
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


    

    #region Helpers
    private async Task<IDistributedLockHandle?> AcquireLockAsync(CancellationToken ct)
        => await distributedLock.TryAcquireAsync(LockName, LockDuration, ct);

    private async Task<CleanseAnalysisOperationDto> CreateOperationAsync(CancellationToken ct)
    {
        var operationId = await operationCommandService.CreateOperationAsync(new CreateOperationCommand(), ct);
        return await operationQueries.GetOperationAsync(operationId, ct)
            ?? throw new InvalidOperationException($"Operation {operationId} was created but could not be retrieved.");
    }

    #endregion

}
