namespace KeeperData.Core.Reports.Operations;

/// <summary>
/// Scoped handle to a mutable <see cref="OperationTreeNode"/>.
/// Passed down the call stack so child methods can create sub-scopes and report progress.
/// </summary>
public sealed class OperationScope
{
    private readonly OperationTreeNode _node;
    private readonly TimeProvider _timeProvider;
    private readonly int _rpmWindowSeconds;

    internal OperationScope(OperationTreeNode node, TimeProvider timeProvider, int rpmWindowSeconds)
    {
        _node = node;
        _timeProvider = timeProvider;
        _rpmWindowSeconds = rpmWindowSeconds;
    }

    /// <summary>
    /// Creates a child scope, adding a new child node to this node's children.
    /// </summary>
    public OperationScope CreateChild(string name)
    {
        var child = new OperationTreeNode(name);
        lock (_node.Lock)
        {
            _node.Children.Add(child);
        }

        return new OperationScope(child, _timeProvider, _rpmWindowSeconds);
    }

    /// <summary>
    /// Marks this scope as in-progress with an optional total record count.
    /// </summary>
    public void Start(int? totalRecords = null, string? description = null)
    {
        lock (_node.Lock)
        {
            _node.Status = OperationStatuses.InProgress;
            _node.StartedAtUtc = _timeProvider.GetUtcNow().UtcDateTime;
            _node.TotalRecords = totalRecords;
            _node.Description = description;
        }
    }

    /// <summary>
    /// Updates progress for this scope.
    /// </summary>
    public void UpdateProgress(int processedCount, string? description = null)
    {
        var now = _timeProvider.GetUtcNow().UtcDateTime;
        lock (_node.Lock)
        {
            _node.ProcessedCount = processedCount;
            if (description is not null)
                _node.Description = description;

            _node.RecordRateSnapshot(now, processedCount);
            _node.PruneOldSnapshots(now, _rpmWindowSeconds);
        }
    }

    /// <summary>
    /// Adds elapsed milliseconds to a sub-path under this scope.
    /// Creates timing-only child nodes as needed (like TimingTree.Track).
    /// </summary>
    public void TrackElapsed(string subPath, long elapsedMs)
    {
        var segments = subPath.Split('/');
        lock (_node.Lock)
        {
            TrackSegments(_node, segments, 0, elapsedMs);
        }
    }

    /// <summary>
    /// Marks this scope as completed and records final elapsed time.
    /// </summary>
    public void Complete(string? description = null)
    {
        FinalizeScope(OperationStatuses.Completed, description);

        lock (_node.Lock)
        {
            if (_node.TotalRecords.HasValue)
                _node.ProcessedCount = _node.TotalRecords.Value;
        }
    }

    /// <summary>
    /// Marks this scope as failed.
    /// </summary>
    public void Fail(string? description = null)
    {
        FinalizeScope(OperationStatuses.Failed, description);
    }

    /// <summary>
    /// Marks this scope as cancelled.
    /// </summary>
    public void Cancel(string? description = null)
    {
        FinalizeScope(OperationStatuses.Cancelled, description);
    }

    private void FinalizeScope(string status, string? description)
    {
        var now = _timeProvider.GetUtcNow().UtcDateTime;
        lock (_node.Lock)
        {
            _node.Status = status;
            if (description is not null)
                _node.Description = description;

            if (_node.StartedAtUtc.HasValue)
            {
                _node.ElapsedMs += (long)(now - _node.StartedAtUtc.Value).TotalMilliseconds;
                _node.StartedAtUtc = null;
            }
        }
    }

    /// <summary>
    /// Captures an immutable snapshot of this scope's node and all descendants.
    /// </summary>
    internal OperationNode Snapshot()
    {
        lock (_node.Lock)
        {
            return _node.CaptureSnapshot(_timeProvider);
        }
    }

    private static void TrackSegments(OperationTreeNode parent, string[] segments, int index, long elapsedMs)
    {
        OperationTreeNode.TrackSegments(parent, segments, index, elapsedMs, markLeafComplete: true);
    }
}
