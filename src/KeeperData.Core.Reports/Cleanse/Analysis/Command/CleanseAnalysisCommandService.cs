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
        Trace.WriteLine("KRDSBRIDGE | StartAnalysisAsync | BEGIN");
        var lockHandle = await AcquireLockAsync(ct);
        if (lockHandle is null)
        {
            Trace.WriteLine("KRDSBRIDGE | StartAnalysisAsync | Lock not acquired, returning null");
            return null;
        }

        var operation = await CreateOperationAsync(ct);
        Trace.WriteLine($"KRDSBRIDGE | StartAnalysisAsync | Operation created, operationId={operation.Id}");

        // Use a long-running thread to avoid thread pool starvation.
        // Do not capture the request-scoped ct here; it will be cancelled when the HTTP request completes.
        _ = Task.Factory.StartNew(
            async () =>
            {
                Trace.WriteLine($"KRDSBRIDGE | StartAnalysisAsync | Background task started, operationId={operation.Id}");
                try
                {
                    await RunAnalysisWithLockAsync(operation, lockHandle, CancellationToken.None);
                    Trace.WriteLine($"KRDSBRIDGE | StartAnalysisAsync | Background task completed, operationId={operation.Id}");
                }
                catch (OperationCanceledException)
                {
                    Trace.WriteLine($"KRDSBRIDGE | StartAnalysisAsync | Background task cancelled, operationId={operation.Id}");
                    logger.LogWarning("Cleanse analysis was cancelled (operationId={OperationId})", operation.Id);
                }
                catch (Exception ex)
                {
                    Trace.WriteLine($"KRDSBRIDGE | StartAnalysisAsync | Background task FAILED, operationId={operation.Id}, error={ex.Message}");
                    logger.LogError(ex, "Background cleanse analysis failed (operationId={OperationId})", operation.Id);
                }
            },
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default
        ).Unwrap();

        Trace.WriteLine($"KRDSBRIDGE | StartAnalysisAsync | END, returning operationId={operation.Id}");
        return operation;
    }

    /// <inheritdoc/>
    public async Task<CleanseAnalysisOperationDto?> RunAnalysisAsync(CancellationToken ct = default)
    {
        Trace.WriteLine("KRDSBRIDGE | RunAnalysisAsync | BEGIN");
        var lockHandle = await AcquireLockAsync(ct);
        if (lockHandle is null)
        {
            Trace.WriteLine("KRDSBRIDGE | RunAnalysisAsync | Lock not acquired, returning null");
            return null;
        }

        var operation = await CreateOperationAsync(ct);
        Trace.WriteLine($"KRDSBRIDGE | RunAnalysisAsync | Operation created, operationId={operation.Id}");
        await RunAnalysisWithLockAsync(operation, lockHandle, ct);
        Trace.WriteLine($"KRDSBRIDGE | RunAnalysisAsync | END, operationId={operation.Id}");
        return await operationQueries.GetOperationAsync(operation.Id, ct);
    }

    /// <inheritdoc/>
    public async Task<bool> CancelAnalysisAsync(CancellationToken ct = default)
    {
        Trace.WriteLine("KRDSBRIDGE | CancelAnalysisAsync | BEGIN");
        var currentOperation = await operationQueries.GetCurrentOperationAsync(ct);
        if (currentOperation is null)
        {
            Trace.WriteLine("KRDSBRIDGE | CancelAnalysisAsync | No current operation found");
            return false;
        }

        await operationCommandService.RequestCancellationAsync(
            new CancelOperationCommand(currentOperation.Id), ct);

        Trace.WriteLine($"KRDSBRIDGE | CancelAnalysisAsync | Cancellation requested, operationId={currentOperation.Id}");
        logger.LogInformation("Cancellation requested for operationId={OperationId}", currentOperation.Id);
        return true;
    }

    private async Task RunAnalysisWithLockAsync(CleanseAnalysisOperationDto operation, IDistributedLockHandle lockHandle, CancellationToken ct)
    {
        Trace.WriteLine($"KRDSBRIDGE | RunAnalysisWithLockAsync | BEGIN, operationId={operation.Id}");
        var stopwatch = Stopwatch.StartNew();
        using var renewalCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var renewalTask = StartLockRenewalAsync(lockHandle, renewalCts.Token);

        try
        {
            var aggregateMetrics = new AnalysisMetrics();

            var metrics = await RunAnalysisPhaseAsync(operation, aggregateMetrics, ct);
            aggregateMetrics.RecordsAnalyzed += metrics.RecordsAnalyzed;
            aggregateMetrics.IssuesFound += metrics.IssuesFound;
            Trace.WriteLine($"KRDSBRIDGE | RunAnalysisWithLockAsync | Analysis phase done, records={metrics.RecordsAnalyzed}, issues={metrics.IssuesFound}, elapsed={stopwatch.ElapsedMilliseconds}ms");

            var deactivatedCount = await RunDeactivationPhaseAsync(operation, ct);
            aggregateMetrics.IssuesResolved += deactivatedCount;
            Trace.WriteLine($"KRDSBRIDGE | RunAnalysisWithLockAsync | Deactivation phase done, deactivated={deactivatedCount}, elapsed={stopwatch.ElapsedMilliseconds}ms");

            await RunExportPhaseAsync(operation, ct);
            Trace.WriteLine($"KRDSBRIDGE | RunAnalysisWithLockAsync | Export phase done, elapsed={stopwatch.ElapsedMilliseconds}ms");

            stopwatch.Stop();
            Trace.WriteLine($"KRDSBRIDGE | RunAnalysisWithLockAsync | All phases completed, operationId={operation.Id}, totalDuration={stopwatch.ElapsedMilliseconds}ms");
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
            Trace.WriteLine($"KRDSBRIDGE | RunAnalysisWithLockAsync | CANCELLED, operationId={operation.Id}, elapsed={stopwatch.ElapsedMilliseconds}ms");
            logger.LogInformation("Cleanse analysis cancelled (operationId={OperationId}), recording cancellation", operation.Id);
            await operationCommandService.CancelOperationAsync(
                new CancelOperationCommand(operation.Id), stopwatch.ElapsedMilliseconds, CancellationToken.None);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            Trace.WriteLine($"KRDSBRIDGE | RunAnalysisWithLockAsync | FAILED, operationId={operation.Id}, error={ex.Message}, elapsed={stopwatch.ElapsedMilliseconds}ms");
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
        Trace.WriteLine($"KRDSBRIDGE | RunAnalysisPhaseAsync | BEGIN, operationId={operation.Id}");
        var phaseStopwatch = Stopwatch.StartNew();
        await operationCommandService.StartPhaseAsync(
            new StartPhaseCommand(operation.Id, OperationPhase.Analysis, 0), ct);

        var phaseTimings = new TimingTree();

        // Background task: periodically persist timing snapshots so the API reflects live progress
        using var timingsCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var timingsReporterTask = RunTimingsReporterAsync(operation.Id, phaseTimings, timingsCts.Token);

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
            phaseTimings,
            ct);

        // Stop the background reporter and do one final persist
        await timingsCts.CancelAsync();
        try { await timingsReporterTask; } catch (OperationCanceledException) { /* expected */ }

        await operationCommandService.UpdateTimingsAsync(
            new UpdateTimingsCommand(operation.Id, phaseTimings.Snapshot("Analysis")), ct);

        await operationCommandService.CompletePhaseAsync(
            new CompletePhaseCommand(operation.Id, OperationPhase.Analysis), ct);
        logger.LogInformation("Phase completed: Analysis (operationId={OperationId}, records={Records}, issues={Issues})",
            operation.Id, metrics.RecordsAnalyzed, metrics.IssuesFound);

        phaseStopwatch.Stop();
        Trace.WriteLine($"KRDSBRIDGE | RunAnalysisPhaseAsync | END, operationId={operation.Id}, records={metrics.RecordsAnalyzed}, issues={metrics.IssuesFound}, duration={phaseStopwatch.ElapsedMilliseconds}ms");
        return metrics;
    }

    private async Task<int> RunDeactivationPhaseAsync(CleanseAnalysisOperationDto operation, CancellationToken ct)
    {
        Trace.WriteLine($"KRDSBRIDGE | RunDeactivationPhaseAsync | BEGIN, operationId={operation.Id}");
        var phaseStopwatch = Stopwatch.StartNew();
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

        phaseStopwatch.Stop();
        Trace.WriteLine($"KRDSBRIDGE | RunDeactivationPhaseAsync | END, operationId={operation.Id}, deactivated={deactivatedCount}, duration={phaseStopwatch.ElapsedMilliseconds}ms");
        return deactivatedCount;
    }

    private async Task RunExportPhaseAsync(CleanseAnalysisOperationDto operation, CancellationToken ct)
    {
        Trace.WriteLine($"KRDSBRIDGE | RunExportPhaseAsync | BEGIN, operationId={operation.Id}");
        var phaseStopwatch = Stopwatch.StartNew();
        await operationCommandService.StartPhaseAsync(
            new StartPhaseCommand(operation.Id, OperationPhase.Export, 0), ct);

        var since = await cleanseReportExportCommandService.GetLastExportedAtUtcAsync(ct);
        logger.LogInformation("Incremental export since={Since} (operationId={OperationId})", since, operation.Id);

        var options = new Export.Command.Domain.ExportOptions { Since = since, SendNotification = true };

        var exportSucceeded = await cleanseReportExportCommandService.ExportReportAsync(
            operation.Id,
            options,
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

        if (exportSucceeded)
        {
            await cleanseReportExportCommandService.RecordSuccessfulExportAsync(ct);
            logger.LogInformation("Recorded successful incremental export timestamp (operationId={OperationId})", operation.Id);
        }

        await operationCommandService.CompletePhaseAsync(
            new CompletePhaseCommand(operation.Id, OperationPhase.Export), ct);
        phaseStopwatch.Stop();
        Trace.WriteLine($"KRDSBRIDGE | RunExportPhaseAsync | END, operationId={operation.Id}, duration={phaseStopwatch.ElapsedMilliseconds}ms");
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

    /// <summary>
    /// Periodically snapshots the timing tree and persists it to MongoDB so
    /// the API reflects live timing data regardless of which phase is executing.
    /// </summary>
    private async Task RunTimingsReporterAsync(string operationId, TimingTree timings, CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(2));
        while (await timer.WaitForNextTickAsync(ct))
        {
            await operationCommandService.UpdateTimingsAsync(
                new UpdateTimingsCommand(operationId, timings.Snapshot("Analysis")), CancellationToken.None);
        }
    }

    #region Helpers
    private async Task<IDistributedLockHandle?> AcquireLockAsync(CancellationToken ct)
    {
        Trace.WriteLine("KRDSBRIDGE | AcquireLockAsync | Attempting lock acquisition");
        var sw = Stopwatch.StartNew();
        var handle = await distributedLock.TryAcquireAsync(LockName, LockDuration, ct);
        sw.Stop();
        Trace.WriteLine($"KRDSBRIDGE | AcquireLockAsync | {(handle is not null ? "Acquired" : "NOT acquired")}, duration={sw.ElapsedMilliseconds}ms");
        return handle;
    }

    private async Task<CleanseAnalysisOperationDto> CreateOperationAsync(CancellationToken ct)
    {
        Trace.WriteLine("KRDSBRIDGE | CreateOperationAsync | Creating operation");
        var sw = Stopwatch.StartNew();
        var operationId = await operationCommandService.CreateOperationAsync(new CreateOperationCommand(), ct);
        var operation = await operationQueries.GetOperationAsync(operationId, ct)
            ?? throw new InvalidOperationException($"Operation {operationId} was created but could not be retrieved.");
        sw.Stop();
        Trace.WriteLine($"KRDSBRIDGE | CreateOperationAsync | Created operationId={operationId}, duration={sw.ElapsedMilliseconds}ms");
        return operation;
    }

    #endregion
}
