using KeeperData.Core.Locking;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Export.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Command;
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
    ICleanseAnalysisOperationAggRootRepository operationRepository,
    ILogger<CleanseAnalysisCommandService> logger,
    ICleanseAnalysisEngine engine) : ICleanseAnalysisCommandService
{
    private const string LockName = "cleanse-analysis";
    private static readonly TimeSpan LockDuration = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan LockRenewalInterval = TimeSpan.FromMinutes(2);

    /// <inheritdoc/>
    public async Task<CleanseAnalysisOperationDto?> StartAnalysisAsync(CancellationToken ct = default)
    {
        Trace.TraceInformation("KRDSBRIDGE | StartAnalysisAsync | BEGIN");
        var lockHandle = await AcquireLockAsync(ct);
        if (lockHandle is null)
        {
            Trace.TraceInformation("KRDSBRIDGE | StartAnalysisAsync | Lock not acquired, returning null");
            return null;
        }

        var operation = await CreateOperationAsync(ct);
        Trace.TraceInformation($"KRDSBRIDGE | StartAnalysisAsync | Operation created, operationId={operation.Id}");

        // Use a long-running thread to avoid thread pool starvation.
        // Do not capture the request-scoped ct here; it will be cancelled when the HTTP request completes.
        _ = Task.Factory.StartNew(
            async () =>
            {
                Trace.TraceInformation($"KRDSBRIDGE | StartAnalysisAsync | Background task started, operationId={operation.Id}");
                try
                {
                    await RunAnalysisWithLockAsync(operation, lockHandle, CancellationToken.None);
                    Trace.TraceInformation($"KRDSBRIDGE | StartAnalysisAsync | Background task completed, operationId={operation.Id}");
                }
                catch (OperationCanceledException)
                {
                    Trace.TraceInformation($"KRDSBRIDGE | StartAnalysisAsync | Background task cancelled, operationId={operation.Id}");
                    logger.LogWarning("Cleanse analysis was cancelled (operationId={OperationId})", operation.Id);
                }
                catch (Exception ex)
                {
                    Trace.TraceInformation($"KRDSBRIDGE | StartAnalysisAsync | Background task FAILED, operationId={operation.Id}, error={ex.Message}");
                    logger.LogError(ex, "Background cleanse analysis failed (operationId={OperationId})", operation.Id);
                }
            },
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default
        ).Unwrap();

        Trace.TraceInformation($"KRDSBRIDGE | StartAnalysisAsync | END, returning operationId={operation.Id}");
        return operation;
    }

    /// <inheritdoc/>
    public async Task<CleanseAnalysisOperationDto?> RunAnalysisAsync(CancellationToken ct = default)
    {
        Trace.TraceInformation("KRDSBRIDGE | RunAnalysisAsync | BEGIN");
        var lockHandle = await AcquireLockAsync(ct);
        if (lockHandle is null)
        {
            Trace.TraceInformation("KRDSBRIDGE | RunAnalysisAsync | Lock not acquired, returning null");
            return null;
        }

        var operation = await CreateOperationAsync(ct);
        Trace.TraceInformation($"KRDSBRIDGE | RunAnalysisAsync | Operation created, operationId={operation.Id}");
        await RunAnalysisWithLockAsync(operation, lockHandle, ct);
        Trace.TraceInformation($"KRDSBRIDGE | RunAnalysisAsync | END, operationId={operation.Id}");
        return await operationQueries.GetOperationAsync(operation.Id, ct);
    }

    /// <inheritdoc/>
    public async Task<bool> CancelAnalysisAsync(CancellationToken ct = default)
    {
        Trace.TraceInformation("KRDSBRIDGE | CancelAnalysisAsync | BEGIN");
        var currentOperation = await operationQueries.GetCurrentOperationAsync(ct);
        if (currentOperation is null)
        {
            Trace.TraceInformation("KRDSBRIDGE | CancelAnalysisAsync | No current operation found");
            return false;
        }

        await operationCommandService.RequestCancellationAsync(
            new CancelOperationCommand(currentOperation.Id), ct);

        Trace.TraceInformation($"KRDSBRIDGE | CancelAnalysisAsync | Cancellation requested, operationId={currentOperation.Id}");
        logger.LogInformation("Cancellation requested for operationId={OperationId}", currentOperation.Id);
        return true;
    }

    private async Task RunAnalysisWithLockAsync(CleanseAnalysisOperationDto operation, IDistributedLockHandle lockHandle, CancellationToken ct)
    {
        Trace.TraceInformation($"KRDSBRIDGE | RunAnalysisWithLockAsync | BEGIN, operationId={operation.Id}");
        var stopwatch = Stopwatch.StartNew();
        using var renewalCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var renewalTask = StartLockRenewalAsync(lockHandle, renewalCts.Token);

        // Shared timing tree accumulated by all phases
        var operationTimings = new TimingTree();

        // Shared in-memory progress tracker — phases mutate this directly, a single
        // background task flushes dirty state + timing snapshots to MongoDB every 2 s.
        var tracker = new OperationProgressTracker(operationRepository);
        await tracker.InitializeAsync(operation.Id, ct);

        using var trackerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var trackerTask = tracker.RunPeriodicFlushAsync(operationTimings, trackerCts.Token);

        try
        {
            var aggregateMetrics = new AnalysisMetrics();

            var metrics = await RunAnalysisPhaseAsync(operation, aggregateMetrics, operationTimings, tracker, ct);
            aggregateMetrics.RecordsAnalyzed += metrics.RecordsAnalyzed;
            aggregateMetrics.IssuesFound += metrics.IssuesFound;
            Trace.TraceInformation($"KRDSBRIDGE | RunAnalysisWithLockAsync | Analysis phase done, records={metrics.RecordsAnalyzed}, issues={metrics.IssuesFound}, elapsed={stopwatch.ElapsedMilliseconds}ms");

            var deactivatedCount = await RunDeactivationPhaseAsync(operation, tracker, ct);
            aggregateMetrics.IssuesResolved += deactivatedCount;
            Trace.TraceInformation($"KRDSBRIDGE | RunAnalysisWithLockAsync | Deactivation phase done, deactivated={deactivatedCount}, elapsed={stopwatch.ElapsedMilliseconds}ms");

            await RunExportPhaseAsync(operation, tracker, ct);
            Trace.TraceInformation($"KRDSBRIDGE | RunAnalysisWithLockAsync | Export phase done, elapsed={stopwatch.ElapsedMilliseconds}ms");

            stopwatch.Stop();
            Trace.TraceInformation($"KRDSBRIDGE | RunAnalysisWithLockAsync | All phases completed, operationId={operation.Id}, totalDuration={stopwatch.ElapsedMilliseconds}ms");
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
            Trace.TraceInformation($"KRDSBRIDGE | RunAnalysisWithLockAsync | CANCELLED, operationId={operation.Id}, elapsed={stopwatch.ElapsedMilliseconds}ms");
            logger.LogInformation("Cleanse analysis cancelled (operationId={OperationId}), recording cancellation", operation.Id);
            await operationCommandService.CancelOperationAsync(
                new CancelOperationCommand(operation.Id), stopwatch.ElapsedMilliseconds, CancellationToken.None);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            Trace.TraceInformation($"KRDSBRIDGE | RunAnalysisWithLockAsync | FAILED, operationId={operation.Id}, error={ex.Message}, elapsed={stopwatch.ElapsedMilliseconds}ms");
            await operationCommandService.FailOperationAsync(new FailOperationCommand(
                operation.Id,
                ex.Message,
                stopwatch.ElapsedMilliseconds), CancellationToken.None);
        }
        finally
        {
            // Stop the periodic flusher and do a final flush with the latest timings
            await trackerCts.CancelAsync();
            try { await trackerTask; } catch (OperationCanceledException) { /* expected */ }
            tracker.UpdateTimings(operationTimings.Snapshot("Analysis"));
            await tracker.FlushAsync(CancellationToken.None);

            runStatsService.ClearSnapshots(operation.Id);
            await renewalCts.CancelAsync();
            try { await renewalTask; } catch { /* Ignore cancellation */ }
            await lockHandle.DisposeAsync();
        }
    }

    private async Task<AnalysisMetrics> RunAnalysisPhaseAsync(
        CleanseAnalysisOperationDto operation, AnalysisMetrics aggregateMetrics,
        TimingTree operationTimings, OperationProgressTracker tracker, CancellationToken ct)
    {
        Trace.TraceInformation($"KRDSBRIDGE | RunAnalysisPhaseAsync | BEGIN, operationId={operation.Id}");
        var phaseStopwatch = Stopwatch.StartNew();
        tracker.StartPhase(OperationPhase.Analysis, 0);

        // Signal that data preloading is in progress so the consumer sees status updates immediately
        tracker.UpdateProgress(0, "Loading reference data...", 0, 0, 0, 0);
        tracker.UpdatePhaseProgress(OperationPhase.Analysis, 0, 0, "Loading reference data...");

        var metrics = await engine.ExecuteAsync(
            operation.Id,
            (recordsAnalyzed, totalRecords, issuesFound, issuesResolved) =>
            {
                if (tracker.IsCancellationRequested)
                {
                    throw new OperationCanceledException("Cancellation requested by user.");
                }

                runStatsService.RecordSnapshot(operation.Id, nameof(OperationPhase.Analysis), recordsAnalyzed);

                tracker.UpdatePhaseProgress(
                    OperationPhase.Analysis,
                    recordsAnalyzed,
                    totalRecords,
                    $"Analyzed {recordsAnalyzed} of {totalRecords} records");

                tracker.UpdateProgress(
                    0,
                    $"Analyzed {recordsAnalyzed} of {totalRecords} records",
                    aggregateMetrics.RecordsAnalyzed + recordsAnalyzed,
                    totalRecords,
                    aggregateMetrics.IssuesFound + issuesFound,
                    aggregateMetrics.IssuesResolved + issuesResolved);

                return Task.CompletedTask;
            },
            operationTimings,
            ct);

        tracker.CompletePhase(OperationPhase.Analysis);
        logger.LogInformation("Phase completed: Analysis (operationId={OperationId}, records={Records}, issues={Issues})",
            operation.Id, metrics.RecordsAnalyzed, metrics.IssuesFound);

        phaseStopwatch.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | RunAnalysisPhaseAsync | END, operationId={operation.Id}, records={metrics.RecordsAnalyzed}, issues={metrics.IssuesFound}, duration={phaseStopwatch.ElapsedMilliseconds}ms");
        return metrics;
    }

    private async Task<int> RunDeactivationPhaseAsync(CleanseAnalysisOperationDto operation,
        OperationProgressTracker tracker, CancellationToken ct)
    {
        Trace.TraceInformation($"KRDSBRIDGE | RunDeactivationPhaseAsync | BEGIN, operationId={operation.Id}");
        var phaseStopwatch = Stopwatch.StartNew();
        tracker.StartPhase(OperationPhase.Deactivation, 0);

        var deactivatedCount = await issueCommandService.DeactivateStaleIssuesAsync(
            new DeactivateStaleIssuesCommand(operation.Id),
            (deactivatedSoFar, totalStale) =>
            {
                runStatsService.RecordSnapshot(operation.Id, nameof(OperationPhase.Deactivation), deactivatedSoFar);

                tracker.UpdatePhaseProgress(
                    OperationPhase.Deactivation,
                    deactivatedSoFar,
                    totalStale,
                    $"Deactivated {deactivatedSoFar} of {totalStale} stale issues");

                return Task.CompletedTask;
            },
            ct);

        tracker.CompletePhase(OperationPhase.Deactivation);
        logger.LogInformation("Phase completed: Deactivation (operationId={OperationId}, deactivated={Deactivated})",
            operation.Id, deactivatedCount);

        phaseStopwatch.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | RunDeactivationPhaseAsync | END, operationId={operation.Id}, deactivated={deactivatedCount}, duration={phaseStopwatch.ElapsedMilliseconds}ms");
        return deactivatedCount;
    }

    private async Task RunExportPhaseAsync(CleanseAnalysisOperationDto operation,
        OperationProgressTracker tracker, CancellationToken ct)
    {
        Trace.TraceInformation($"KRDSBRIDGE | RunExportPhaseAsync | BEGIN, operationId={operation.Id}");
        var phaseStopwatch = Stopwatch.StartNew();
        tracker.StartPhase(OperationPhase.Export, 0);

        var since = await cleanseReportExportCommandService.GetLastExportedAtUtcAsync(ct);
        logger.LogInformation("Incremental export since={Since} (operationId={OperationId})", since, operation.Id);

        var options = new Export.Command.Domain.ExportOptions { Since = since, SendNotification = true };

        var exportSucceeded = await cleanseReportExportCommandService.ExportReportAsync(
            operation.Id,
            options,
            (recordsProcessed, totalRecords, stepDescription) =>
            {
                runStatsService.RecordSnapshot(operation.Id, nameof(OperationPhase.Export), recordsProcessed);

                tracker.UpdatePhaseProgress(
                    OperationPhase.Export,
                    recordsProcessed,
                    totalRecords,
                    stepDescription);

                return Task.CompletedTask;
            },
            ct);

        if (exportSucceeded)
        {
            await cleanseReportExportCommandService.RecordSuccessfulExportAsync(ct);
            logger.LogInformation("Recorded successful incremental export timestamp (operationId={OperationId})", operation.Id);
        }

        tracker.CompletePhase(OperationPhase.Export);
        phaseStopwatch.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | RunExportPhaseAsync | END, operationId={operation.Id}, duration={phaseStopwatch.ElapsedMilliseconds}ms");
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
    {
        Trace.TraceInformation("KRDSBRIDGE | AcquireLockAsync | Attempting lock acquisition");
        var sw = Stopwatch.StartNew();
        var handle = await distributedLock.TryAcquireAsync(LockName, LockDuration, ct);
        sw.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | AcquireLockAsync | {(handle is not null ? "Acquired" : "NOT acquired")}, duration={sw.ElapsedMilliseconds}ms");
        return handle;
    }

    private async Task<CleanseAnalysisOperationDto> CreateOperationAsync(CancellationToken ct)
    {
        Trace.TraceInformation("KRDSBRIDGE | CreateOperationAsync | Creating operation");
        var sw = Stopwatch.StartNew();
        var operationId = await operationCommandService.CreateOperationAsync(new CreateOperationCommand(), ct);
        var operation = await operationQueries.GetOperationAsync(operationId, ct)
            ?? throw new InvalidOperationException($"Operation {operationId} was created but could not be retrieved.");
        sw.Stop();
        Trace.TraceInformation($"KRDSBRIDGE | CreateOperationAsync | Created operationId={operationId}, duration={sw.ElapsedMilliseconds}ms");
        return operation;
    }

    #endregion
}
