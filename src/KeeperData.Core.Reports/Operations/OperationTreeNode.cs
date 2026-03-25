namespace KeeperData.Core.Reports.Operations;

/// <summary>
/// Internal mutable backing node in the operation tree.
/// Tracks timing, progress counts, and a ring buffer for windowed RPM calculation.
/// All mutations must be performed under the node's <see cref="Lock"/>.
/// </summary>
internal sealed class OperationTreeNode
{
    private const int RingBufferCapacity = 120;

    private readonly record struct RateSnapshot(DateTime TimestampUtc, int RecordsProcessed);

    private readonly RateSnapshot[] _rateBuffer = new RateSnapshot[RingBufferCapacity];
    private int _rateHead;
    private int _rateCount;

    internal readonly object Lock = new();

    public string Name { get; }
    public string Status { get; set; } = OperationStatuses.NotStarted;
    public string? Description { get; set; }
    public int? TotalRecords { get; set; }
    public int ProcessedCount { get; set; }
    public long ElapsedMs { get; set; }
    public DateTime? StartedAtUtc { get; set; }
    public List<OperationTreeNode> Children { get; } = [];

    public OperationTreeNode(string name)
    {
        Name = name;
    }

    /// <summary>
    /// Records a rate snapshot for windowed RPM calculation.
    /// Must be called under <see cref="Lock"/>.
    /// </summary>
    public void RecordRateSnapshot(DateTime timestampUtc, int recordsProcessed)
    {
        var index = (_rateHead + _rateCount) % RingBufferCapacity;
        _rateBuffer[index] = new RateSnapshot(timestampUtc, recordsProcessed);

        if (_rateCount < RingBufferCapacity)
            _rateCount++;
        else
            _rateHead = (_rateHead + 1) % RingBufferCapacity;
    }

    /// <summary>
    /// Prunes snapshots older than <paramref name="windowSeconds"/> from the ring buffer.
    /// Must be called under <see cref="Lock"/>.
    /// </summary>
    public void PruneOldSnapshots(DateTime nowUtc, int windowSeconds)
    {
        var cutoff = nowUtc.AddSeconds(-windowSeconds);
        while (_rateCount > 0)
        {
            var oldest = _rateBuffer[_rateHead];
            if (oldest.TimestampUtc >= cutoff)
                break;
            _rateHead = (_rateHead + 1) % RingBufferCapacity;
            _rateCount--;
        }
    }

    /// <summary>
    /// Calculates windowed RPM from the current ring buffer contents.
    /// Must be called under <see cref="Lock"/>.
    /// </summary>
    public double CalculateWindowRpm()
    {
        if (_rateCount < 2)
            return 0;

        var oldest = _rateBuffer[_rateHead];
        var newestIndex = (_rateHead + _rateCount - 1) % RingBufferCapacity;
        var newest = _rateBuffer[newestIndex];

        var windowMinutes = (newest.TimestampUtc - oldest.TimestampUtc).TotalMinutes;
        if (windowMinutes <= 0)
            return 0;

        var recordsDelta = newest.RecordsProcessed - oldest.RecordsProcessed;
        return Math.Round(recordsDelta / windowMinutes, 2);
    }

    /// <summary>
    /// Produces an immutable deep-copy snapshot of this node and all its children.
    /// Must be called under <see cref="Lock"/> for this node; acquires child locks internally.
    /// </summary>
    public OperationNode CaptureSnapshot(TimeProvider timeProvider)
    {
        var now = timeProvider.GetUtcNow().UtcDateTime;

        var elapsedMs = ElapsedMs;
        if (Status == OperationStatuses.InProgress && StartedAtUtc.HasValue)
        {
            elapsedMs += (long)(now - StartedAtUtc.Value).TotalMilliseconds;
        }

        List<OperationNode>? childSnapshots = null;
        if (Children.Count > 0)
        {
            childSnapshots = new List<OperationNode>(Children.Count);
            foreach (var child in Children)
            {
                lock (child.Lock)
                {
                    childSnapshots.Add(child.CaptureSnapshot(timeProvider));
                }
            }
        }

        // Compute progress percentage
        double? percentComplete = ComputePercentComplete(childSnapshots);

        // Compute rate metrics
        var currentRpm = CalculateWindowRpm();
        var averageRpm = ComputeAverageRpm(now);

        // Compute projections
        long? projectedRemainingMs = null;
        DateTime? projectedEndTimeUtc = null;
        if (TotalRecords.HasValue && TotalRecords.Value > 0)
        {
            var remaining = TotalRecords.Value - ProcessedCount;
            if (remaining > 0)
            {
                var projectionRpm = currentRpm > 0 ? currentRpm : averageRpm;
                if (projectionRpm > 0)
                {
                    var remainingMinutes = remaining / projectionRpm;
                    projectedRemainingMs = (long)(remainingMinutes * 60_000);
                    projectedEndTimeUtc = now.AddMinutes(remainingMinutes);
                }
            }
        }

        return new OperationNode
        {
            Name = Name,
            Status = Status,
            Description = Description,
            PercentComplete = percentComplete,
            ProcessedCount = TotalRecords.HasValue ? ProcessedCount : null,
            TotalRecords = TotalRecords,
            ElapsedMs = elapsedMs > 0 ? elapsedMs : RollUpChildElapsed(childSnapshots),
            Elapsed = OperationNode.FormatElapsed(elapsedMs > 0 ? elapsedMs : RollUpChildElapsed(childSnapshots)),
            ProjectedRemainingMs = projectedRemainingMs,
            ProjectedEndTimeUtc = projectedEndTimeUtc,
            CurrentRecordsPerMinute = currentRpm > 0 ? currentRpm : null,
            AverageRecordsPerMinute = averageRpm > 0 ? averageRpm : null,
            Children = childSnapshots
        };
    }

    private double? ComputePercentComplete(List<OperationNode>? childSnapshots)
    {
        // Leaf node with own totalRecords
        if (TotalRecords.HasValue && TotalRecords.Value > 0)
            return Math.Round(100.0 * ProcessedCount / TotalRecords.Value, 2);

        if (Status == OperationStatuses.Completed)
            return 100;

        // Parent: weighted average by each child's totalRecords
        if (childSnapshots is { Count: > 0 })
        {
            var totalWeight = 0L;
            var weightedSum = 0.0;

            foreach (var child in childSnapshots)
            {
                var weight = child.TotalRecords ?? 1;
                totalWeight += weight;
                weightedSum += (child.PercentComplete ?? 0) * weight;
            }

            if (totalWeight > 0)
                return Math.Round(weightedSum / totalWeight, 2);
        }

        return null;
    }

    private double ComputeAverageRpm(DateTime nowUtc)
    {
        if (!StartedAtUtc.HasValue || ProcessedCount <= 0)
            return 0;

        var elapsedMinutes = (nowUtc - StartedAtUtc.Value).TotalMinutes;
        return elapsedMinutes > 0
            ? Math.Round(ProcessedCount / elapsedMinutes, 2)
            : 0;
    }

    private static long RollUpChildElapsed(List<OperationNode>? children)
    {
        if (children is null or { Count: 0 })
            return 0;

        return children.Sum(c => c.ElapsedMs);
    }
}
