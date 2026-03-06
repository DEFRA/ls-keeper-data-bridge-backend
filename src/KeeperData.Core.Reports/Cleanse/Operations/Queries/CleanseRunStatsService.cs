using System.Collections.Concurrent;
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
    private readonly record struct Snapshot(DateTime TimestampUtc, int RecordsAnalyzed);

    private readonly ConcurrentDictionary<string, ConcurrentQueue<Snapshot>> _snapshots = new();

    /// <inheritdoc/>
    public void RecordSnapshot(string operationId, int recordsAnalyzed)
    {
        var queue = _snapshots.GetOrAdd(operationId, _ => new ConcurrentQueue<Snapshot>());
        queue.Enqueue(new Snapshot(timeProvider.GetUtcNow().UtcDateTime, recordsAnalyzed));

        PruneOldSnapshots(queue, throttler.Settings.CleanseAnalysis.RpmWindowSeconds);
    }

    /// <inheritdoc/>
    public CleanseRunStatsDto? CalculateStats(string operationId, int recordsAnalyzed, int totalRecords, DateTime startedAtUtc)
    {
        var settings = throttler.Settings.CleanseAnalysis;
        var now = timeProvider.GetUtcNow().UtcDateTime;
        var elapsedMinutes = (now - startedAtUtc).TotalMinutes;

        var averageRpm = elapsedMinutes > 0
            ? Math.Round(recordsAnalyzed / elapsedMinutes, 2)
            : 0;

        double currentRpm = 0;
        if (_snapshots.TryGetValue(operationId, out var queue))
        {
            PruneOldSnapshots(queue, settings.RpmWindowSeconds);
            currentRpm = CalculateWindowRpm(queue);
        }

        DateTime? projectedEnd = null;
        var remainingRecords = totalRecords - recordsAnalyzed;
        if (averageRpm > 0 && remainingRecords > 0)
        {
            var remainingMinutes = remainingRecords / averageRpm;
            projectedEnd = now.AddMinutes(remainingMinutes);
        }

        return new CleanseRunStatsDto
        {
            CurrentRpm = currentRpm,
            AverageRpm = averageRpm,
            ProjectedEndUtc = projectedEnd,
            ThrottlePolicyName = throttler.ActivePolicyName,
            ThrottlePolicySlug = throttler.ActivePolicySlug,
            PumpBatchSize = settings.PumpBatchSize,
            PumpDelayMs = settings.PumpDelayMs
        };
    }

    /// <inheritdoc/>
    public void ClearSnapshots(string operationId)
    {
        _snapshots.TryRemove(operationId, out _);
    }

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

        var recordsDelta = newest.RecordsAnalyzed - oldest.RecordsAnalyzed;
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
