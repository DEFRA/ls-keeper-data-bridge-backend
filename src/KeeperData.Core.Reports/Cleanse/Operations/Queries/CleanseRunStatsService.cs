using System.Collections.Concurrent;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Operations.Queries.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Queries.Dtos;
using KeeperData.Core.Throttling;

namespace KeeperData.Core.Reports.Cleanse.Operations.Queries;

/// <summary>
/// Singleton service that tracks in-memory sliding-window snapshots for computing live RPM
/// and projected end times during cleanse analysis operations.
/// </summary>
public sealed class CleanseRunStatsService(IThrottler throttler, TimeProvider timeProvider) : ICleanseRunStatsService
{
    private readonly record struct Snapshot(DateTime TimestampUtc, int RecordsProcessed);

    private readonly ConcurrentDictionary<string, ConcurrentQueue<Snapshot>> _snapshots = new();

    /// <inheritdoc/>
    public void RecordSnapshot(string operationId, int recordsAnalyzed)
    {
        RecordSnapshot(operationId, OperationPhase.Analysis.ToString(), recordsAnalyzed);
    }

    /// <inheritdoc/>
    public void RecordSnapshot(string operationId, string phaseName, int recordsProcessed)
    {
        var key = BuildKey(operationId, phaseName);
        var windowSeconds = GetRpmWindowSeconds(phaseName);
        var queue = _snapshots.GetOrAdd(key, _ => new ConcurrentQueue<Snapshot>());
        queue.Enqueue(new Snapshot(timeProvider.GetUtcNow().UtcDateTime, recordsProcessed));
        PruneOldSnapshots(queue, windowSeconds);
    }

    /// <inheritdoc/>
    public CleanseRunStatsDto? CalculateStats(string operationId, int recordsAnalyzed, int totalRecords, DateTime startedAtUtc)
    {
        var phaseStats = CalculatePhaseStats(operationId, OperationPhase.Analysis.ToString(), recordsAnalyzed, totalRecords, startedAtUtc);
        if (phaseStats is null)
            return null;

        var settings = throttler.Settings.CleanseAnalysis;
        return new CleanseRunStatsDto
        {
            CurrentRpm = phaseStats.CurrentRpm,
            AverageRpm = phaseStats.AverageRpm,
            ProjectedEndUtc = phaseStats.ProjectedEndUtc,
            EstimatedDurationRemainingSeconds = phaseStats.EstimatedRemainingSeconds,
            ThrottlePolicyName = phaseStats.ThrottlePolicyName,
            ThrottlePolicySlug = phaseStats.ThrottlePolicySlug,
            PumpBatchSize = settings.PumpBatchSize,
            PumpDelayMs = settings.PumpDelayMs
        };
    }

    /// <inheritdoc/>
    public PhaseStats? CalculatePhaseStats(string operationId, string phaseName, int recordsProcessed, int totalRecords, DateTime phaseStartedAtUtc)
    {
        var key = BuildKey(operationId, phaseName);
        var windowSeconds = GetRpmWindowSeconds(phaseName);
        var now = timeProvider.GetUtcNow().UtcDateTime;
        var elapsedMinutes = (now - phaseStartedAtUtc).TotalMinutes;

        var averageRpm = elapsedMinutes > 0
            ? Math.Round(recordsProcessed / elapsedMinutes, 2)
            : 0;

        double currentRpm = 0;
        if (_snapshots.TryGetValue(key, out var queue))
        {
            PruneOldSnapshots(queue, windowSeconds);
            currentRpm = CalculateWindowRpm(queue);
        }

        DateTime? projectedEnd = null;
        double? estimatedRemainingSeconds = null;
        var remainingRecords = totalRecords - recordsProcessed;

        var projectionRpm = currentRpm > 0 ? currentRpm : averageRpm;
        if (projectionRpm > 0 && remainingRecords > 0)
        {
            var remainingMinutes = remainingRecords / projectionRpm;
            projectedEnd = now.AddMinutes(remainingMinutes);
            estimatedRemainingSeconds = Math.Round(remainingMinutes * 60, 1);
        }

        var (batchSize, batchDelayMs) = GetThrottleSettings(phaseName);

        return new PhaseStats
        {
            CurrentRpm = currentRpm,
            AverageRpm = averageRpm,
            ProjectedEndUtc = projectedEnd,
            EstimatedRemainingSeconds = estimatedRemainingSeconds,
            ThrottlePolicyName = throttler.ActivePolicyName,
            ThrottlePolicySlug = throttler.ActivePolicySlug,
            BatchSize = batchSize,
            BatchDelayMs = batchDelayMs
        };
    }

    /// <inheritdoc/>
    public void ClearSnapshots(string operationId)
    {
        // Remove all phase-keyed entries for this operation
        var keysToRemove = _snapshots.Keys.Where(k => k.StartsWith($"{operationId}:", StringComparison.Ordinal)).ToList();
        foreach (var key in keysToRemove)
        {
            _snapshots.TryRemove(key, out _);
        }
        // Also remove legacy non-phased key
        _snapshots.TryRemove(operationId, out _);
    }

    private static string BuildKey(string operationId, string phaseName) => $"{operationId}:{phaseName}";

    private int GetRpmWindowSeconds(string phaseName) => phaseName switch
    {
        nameof(OperationPhase.Deactivation) => throttler.Settings.IssueDeactivation.RpmWindowSeconds,
        nameof(OperationPhase.Export) => throttler.Settings.CleanseExport.RpmWindowSeconds,
        _ => throttler.Settings.CleanseAnalysis.RpmWindowSeconds
    };

    private (int BatchSize, int BatchDelayMs) GetThrottleSettings(string phaseName) => phaseName switch
    {
        nameof(OperationPhase.Deactivation) => (throttler.Settings.IssueDeactivation.BatchSize, throttler.Settings.IssueDeactivation.ThrottleDelayMs),
        nameof(OperationPhase.Export) => (throttler.Settings.CleanseExport.StreamBatchSize, throttler.Settings.CleanseExport.ThrottlingDelayMs),
        _ => (throttler.Settings.CleanseAnalysis.PumpBatchSize, throttler.Settings.CleanseAnalysis.PumpDelayMs)
    };

    private static double CalculateWindowRpm(ConcurrentQueue<Snapshot> queue)
    {
        var snapshots = queue.ToArray();
        if (snapshots.Length < 2)
            return 0;

        var oldest = snapshots[0];
        var newest = snapshots[^1];
        var windowMinutes = (newest.TimestampUtc - oldest.TimestampUtc).TotalMinutes;

        if (windowMinutes <= 0)
            return 0;

        var recordsDelta = newest.RecordsProcessed - oldest.RecordsProcessed;
        return Math.Round(recordsDelta / windowMinutes, 2);
    }

    private void PruneOldSnapshots(ConcurrentQueue<Snapshot> queue, int windowSeconds)
    {
        var cutoff = timeProvider.GetUtcNow().UtcDateTime.AddSeconds(-windowSeconds);
        while (queue.TryPeek(out var oldest) && oldest.TimestampUtc < cutoff)
        {
            queue.TryDequeue(out _);
        }
    }
}
