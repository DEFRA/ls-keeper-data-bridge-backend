using System.Diagnostics;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Locking;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Reports.Abstract;
using KeeperData.Core.Reports.Analysis;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Dtos;
using KeeperData.Core.Reports.Strategies;

namespace KeeperData.Core.Reports;

/// <summary>
/// Service for running cleanse analysis and managing cleanse report data.
/// Orchestrates analysis by delegating to registered strategies.
/// </summary>
public class CleanseReportService(
    IQueryService queryService,
    DataSetDefinitions dataSets,
    ICleanseReportRepository reportRepository,
    ICleanseAnalysisRepository analysisRepository,
    IDistributedLock distributedLock,
    IEnumerable<ICleanseAnalysisStrategy> strategies) : ICleanseReportService
{
    private const string LockName = "cleanse-analysis";
    private static readonly TimeSpan LockDuration = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan LockRenewalInterval = TimeSpan.FromMinutes(2);

    public async Task<CleanseAnalysisOperation?> StartAnalysisAsync(CancellationToken ct = default)
    {
        var lockHandle = await AcquireLockAsync(ct);
        if (lockHandle is null)
            return null;

        var operation = await CreateOperationAsync(ct);
        _ = RunAnalysisWithLockAsync(operation, lockHandle, ct);
        return operation;
    }

    public Task<CleanseAnalysisOperation?> GetOperationAsync(string operationId, CancellationToken ct = default)
        => analysisRepository.GetOperationAsync(operationId, ct);

    public Task<IReadOnlyList<CleanseAnalysisOperationSummary>> GetOperationsAsync(int skip = 0, int top = 10, CancellationToken ct = default)
        => analysisRepository.GetOperationsAsync(skip, top, ct);

    public Task<CleanseAnalysisOperation?> GetCurrentOperationAsync(CancellationToken ct = default)
        => analysisRepository.GetCurrentOperationAsync(ct);

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
