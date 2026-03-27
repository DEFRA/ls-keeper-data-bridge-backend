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

        // Check whether children carry deterministic progress (their own TotalRecords)
        var childrenHaveProgress = HasChildrenWithProgress(childSnapshots);

        // Compute progress percentage — prefer child aggregate when children have TotalRecords
        double? percentComplete = ComputePercentComplete(childSnapshots, childrenHaveProgress);

        // Roll up ProcessedCount / TotalRecords from children when applicable
        var (snapshotProcessed, snapshotTotal) = childrenHaveProgress
            ? AggregateChildCounts(childSnapshots!)
            : (TotalRecords.HasValue ? ProcessedCount : (int?)null, TotalRecords);

        // Compute rate metrics — roll up from children when the parent has no ring buffer data
        var currentRpm = CalculateWindowRpm();
        var averageRpm = ComputeAverageRpm(now);
        if (currentRpm <= 0 && childrenHaveProgress)
            currentRpm = RollUpChildRpm(childSnapshots!, c => c.CurrentRecordsPerMinute);
        if (averageRpm <= 0 && childrenHaveProgress)
            averageRpm = RollUpChildRpm(childSnapshots!, c => c.AverageRecordsPerMinute);

        // Compute projections (only meaningful at leaf level with own rate history)
        var (projectedRemainingMs, projectedEndTimeUtc) = ComputeProjections(now, CalculateWindowRpm(), ComputeAverageRpm(now));

        return new OperationNode
        {
            Name = Name,
            Status = Status,
            Description = Description,
            PercentComplete = percentComplete,
            ProcessedCount = snapshotProcessed,
            TotalRecords = snapshotTotal,
            ElapsedMs = elapsedMs > 0 ? elapsedMs : RollUpChildElapsed(childSnapshots),
            Elapsed = OperationNode.FormatElapsed(elapsedMs > 0 ? elapsedMs : RollUpChildElapsed(childSnapshots)),
            ProjectedRemainingMs = projectedRemainingMs,
            ProjectedEndTimeUtc = projectedEndTimeUtc,
            CurrentRecordsPerMinute = currentRpm > 0 ? currentRpm : null,
            AverageRecordsPerMinute = averageRpm > 0 ? averageRpm : null,
            Children = childSnapshots
        };
    }

    private double? ComputePercentComplete(List<OperationNode>? childSnapshots, bool childrenHaveProgress)
    {
        // When children carry deterministic progress, prefer the weighted aggregate
        // over the parent's own (potentially stale) ProcessedCount.
        if (childrenHaveProgress)
            return ComputeWeightedChildPercent(childSnapshots!);

        // Leaf node with own totalRecords (no children with progress)
        if (TotalRecords.HasValue && TotalRecords.Value > 0)
            return Math.Round(100.0 * ProcessedCount / TotalRecords.Value, 2);

        if (Status == OperationStatuses.Completed)
            return 100;

        // Parent with children that lack TotalRecords — use equal weight
        if (childSnapshots is { Count: > 0 })
            return ComputeWeightedChildPercent(childSnapshots);

        return null;
    }

    private static double? ComputeWeightedChildPercent(List<OperationNode> childSnapshots)
    {
        var totalWeight = 0L;
        var weightedSum = 0.0;

        foreach (var child in childSnapshots)
        {
            var weight = child.TotalRecords ?? 1;
            totalWeight += weight;
            weightedSum += (child.PercentComplete ?? 0) * weight;
        }

        return totalWeight > 0 ? Math.Round(weightedSum / totalWeight, 2) : null;
    }

    private static bool HasChildrenWithProgress(List<OperationNode>? childSnapshots)
    {
        if (childSnapshots is not { Count: > 0 })
            return false;

        return childSnapshots.Any(c => c.TotalRecords.HasValue && c.TotalRecords.Value > 0);
    }

    private static (int? ProcessedCount, int? TotalRecords) AggregateChildCounts(List<OperationNode> childSnapshots)
    {
        var totalRecords = 0;
        var processedCount = 0;

        foreach (var child in childSnapshots)
        {
            totalRecords += child.TotalRecords ?? 0;
            processedCount += child.ProcessedCount ?? 0;
        }

        return (processedCount, totalRecords > 0 ? totalRecords : null);
    }

    private static double RollUpChildRpm(List<OperationNode> childSnapshots, Func<OperationNode, double?> selector)
    {
        var sum = 0.0;
        foreach (var child in childSnapshots)
            sum += selector(child) ?? 0;

        return Math.Round(sum, 2);
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

    private (long? RemainingMs, DateTime? EndTimeUtc) ComputeProjections(
        DateTime nowUtc, double currentRpm, double averageRpm)
    {
        if (!TotalRecords.HasValue || TotalRecords.Value <= 0)
            return (null, null);

        var remaining = TotalRecords.Value - ProcessedCount;
        if (remaining <= 0)
            return (null, null);

        var projectionRpm = currentRpm > 0 ? currentRpm : averageRpm;
        if (projectionRpm <= 0)
            return (null, null);

        var remainingMinutes = remaining / projectionRpm;
        return ((long)(remainingMinutes * 60_000), nowUtc.AddMinutes(remainingMinutes));
    }

    private static long RollUpChildElapsed(List<OperationNode>? children)
    {
        if (children is null or { Count: 0 })
            return 0;

        return children.Sum(c => c.ElapsedMs);
    }

    /// <summary>
    /// Walks or creates child nodes along <paramref name="segments"/> and accumulates
    /// elapsed time on the leaf. Must be called under the parent's <see cref="Lock"/>.
    /// </summary>
    internal static void TrackSegments(OperationTreeNode parent, string[] segments, int index, long elapsedMs, bool markLeafComplete)
    {
        var name = segments[index];
        var child = parent.Children.Find(c => c.Name == name);
        if (child is null)
        {
            child = new OperationTreeNode(name);
            parent.Children.Add(child);
        }

        if (index == segments.Length - 1)
        {
            child.ElapsedMs += elapsedMs;
            if (markLeafComplete)
                child.Status = OperationStatuses.Completed;
        }
        else
        {
            TrackSegments(child, segments, index + 1, elapsedMs, markLeafComplete);
        }
    }
}
