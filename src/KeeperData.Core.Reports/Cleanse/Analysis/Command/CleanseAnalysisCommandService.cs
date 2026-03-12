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
    ICleanseRunStatsService runStatsService,
    ILogger<CleanseAnalysisCommandService> logger,
    ICleanseAnalysisEngine engine) : ICleanseAnalysisCommandService
{
    private const string LockName = "cleanse-analysis";
    private static readonly TimeSpan LockDuration = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan LockRenewalInterval = TimeSpan.FromMinutes(2);

    /// <inheritdoc/>
    public async Task<CleanseAnalysisOperationDto?> StartAnalysisAsync(CancellationToken ct = default)
    {
        var lockHandle = await AcquireLockAsync(ct);
        if (lockHandle is null)
            return null;

        var operation = await CreateOperationAsync(ct);

        // Use a long-running thread to avoid thread pool starvation.
        // Do not capture the request-scoped ct here; it will be cancelled when the HTTP request completes.
        _ = Task.Factory.StartNew(
            async () =>
            {
                try
                {
                    await RunAnalysisWithLockAsync(operation, lockHandle, CancellationToken.None);
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

    /// <inheritdoc/>
    public async Task<CleanseAnalysisOperationDto?> RunAnalysisAsync(CancellationToken ct = default)
    {
        var lockHandle = await AcquireLockAsync(ct);
        if (lockHandle is null)
            return null;

        var operation = await CreateOperationAsync(ct);
        await RunAnalysisWithLockAsync(operation, lockHandle, ct);
        return await operationQueries.GetOperationAsync(operation.Id, ct);
    }

    /// <inheritdoc/>
    public async Task<bool> CancelAnalysisAsync(CancellationToken ct = default)
    {
        var currentOperation = await operationQueries.GetCurrentOperationAsync(ct);
        if (currentOperation is null)
            return false;

        await operationCommandService.RequestCancellationAsync(
            new CancelOperationCommand(currentOperation.Id), ct);

        logger.LogInformation("Cancellation requested for operationId={OperationId}", currentOperation.Id);
        return true;
    }

    private async Task RunAnalysisWithLockAsync(CleanseAnalysisOperationDto operation, IDistributedLockHandle lockHandle, CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        using var renewalCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var renewalTask = StartLockRenewalAsync(lockHandle, renewalCts.Token);

        try
        {
            var aggregateMetrics = new AnalysisMetrics();

            var metrics = await RunAnalysisPhaseAsync(operation, aggregateMetrics, ct);
            aggregateMetrics.RecordsAnalyzed += metrics.RecordsAnalyzed;
            aggregateMetrics.IssuesFound += metrics.IssuesFound;

            var deactivatedCount = await RunDeactivationPhaseAsync(operation, ct);
            aggregateMetrics.IssuesResolved += deactivatedCount;

            await RunExportPhaseAsync(operation, ct);

            stopwatch.Stop();
            await operationCommandService.CompleteOperationAsync(new CompleteOperationCommand(
                operation.Id,
                metrics.RecordsAnalyzed,
                metrics.IssuesFound,
                aggregateMetrics.IssuesResolved,
                stopwatch.ElapsedMilliseconds), ct);
        }
        catch (OperationCanceledException)
        {
            stopwatch.Stop();
            logger.LogInformation("Cleanse analysis cancelled (operationId={OperationId}), recording cancellation", operation.Id);
            await operationCommandService.CancelOperationAsync(
                new CancelOperationCommand(operation.Id), stopwatch.ElapsedMilliseconds, CancellationToken.None);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await operationCommandService.FailOperationAsync(new FailOperationCommand(
                operation.Id,
                ex.Message,
                stopwatch.ElapsedMilliseconds), CancellationToken.None);
        }
        finally
        {
            runStatsService.ClearSnapshots(operation.Id);
            await renewalCts.CancelAsync();
            try { await renewalTask; } catch { /* Ignore cancellation */ }
            await lockHandle.DisposeAsync();
        }
    }

    private async Task<AnalysisMetrics> RunAnalysisPhaseAsync(
        CleanseAnalysisOperationDto operation, AnalysisMetrics aggregateMetrics, CancellationToken ct)
    {
        await operationCommandService.StartPhaseAsync(
            new StartPhaseCommand(operation.Id, OperationPhase.Analysis, 0), ct);

        var metrics = await engine.ExecuteAsync(
            operation.Id,
            async (recordsAnalyzed, totalRecords, issuesFound, issuesResolved) =>
            {
                if (await operationCommandService.IsCancellationRequestedAsync(operation.Id, ct))
                {
                    throw new OperationCanceledException("Cancellation requested by user.");
                }

                runStatsService.RecordSnapshot(operation.Id, nameof(OperationPhase.Analysis), recordsAnalyzed);

                await operationCommandService.UpdatePhaseProgressAsync(new UpdatePhaseProgressCommand(
                    operation.Id,
                    OperationPhase.Analysis,
                    recordsAnalyzed,
                    totalRecords,
                    $"Analyzed {recordsAnalyzed} of {totalRecords} records"), ct);

                await operationCommandService.UpdateProgressAsync(new UpdateProgressCommand(
                    operation.Id,
                    0,
                    $"Analyzed {recordsAnalyzed} of {totalRecords} records",
                    aggregateMetrics.RecordsAnalyzed + recordsAnalyzed,
                    totalRecords,
                    aggregateMetrics.IssuesFound + issuesFound,
                    aggregateMetrics.IssuesResolved + issuesResolved), ct);
            },
            ct);

        await operationCommandService.CompletePhaseAsync(
            new CompletePhaseCommand(operation.Id, OperationPhase.Analysis), ct);
        logger.LogInformation("Phase completed: Analysis (operationId={OperationId}, records={Records}, issues={Issues})",
            operation.Id, metrics.RecordsAnalyzed, metrics.IssuesFound);

        return metrics;
    }

    private async Task<int> RunDeactivationPhaseAsync(CleanseAnalysisOperationDto operation, CancellationToken ct)
    {
        await operationCommandService.StartPhaseAsync(
            new StartPhaseCommand(operation.Id, OperationPhase.Deactivation, 0), ct);

        var deactivatedCount = await issueCommandService.DeactivateStaleIssuesAsync(
            new DeactivateStaleIssuesCommand(operation.Id),
            async (deactivatedSoFar, totalStale) =>
            {
                runStatsService.RecordSnapshot(operation.Id, nameof(OperationPhase.Deactivation), deactivatedSoFar);

                await operationCommandService.UpdatePhaseProgressAsync(new UpdatePhaseProgressCommand(
                    operation.Id,
                    OperationPhase.Deactivation,
                    deactivatedSoFar,
                    totalStale,
                    $"Deactivated {deactivatedSoFar} of {totalStale} stale issues"), ct);
            },
            ct);

        await operationCommandService.CompletePhaseAsync(
            new CompletePhaseCommand(operation.Id, OperationPhase.Deactivation), ct);
        logger.LogInformation("Phase completed: Deactivation (operationId={OperationId}, deactivated={Deactivated})",
            operation.Id, deactivatedCount);

        return deactivatedCount;
    }

    private async Task RunExportPhaseAsync(CleanseAnalysisOperationDto operation, CancellationToken ct)
    {
        await operationCommandService.StartPhaseAsync(
            new StartPhaseCommand(operation.Id, OperationPhase.Export, 0), ct);

        await cleanseReportExportCommandService.ExportReportAsync(
            operation.Id,
            async (recordsProcessed, totalRecords, stepDescription) =>
            {
                runStatsService.RecordSnapshot(operation.Id, nameof(OperationPhase.Export), recordsProcessed);

                await operationCommandService.UpdatePhaseProgressAsync(new UpdatePhaseProgressCommand(
                    operation.Id,
                    OperationPhase.Export,
                    recordsProcessed,
                    totalRecords,
                    stepDescription), ct);
            },
            ct);

        await operationCommandService.CompletePhaseAsync(
            new CompletePhaseCommand(operation.Id, OperationPhase.Export), ct);
        logger.LogInformation("Phase completed: Export (operationId={OperationId})", operation.Id);
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
