namespace KeeperData.Core.Reports.Operations;

/// <summary>
/// Thread-safe, mutable accumulator that builds a hierarchical tree of operation data
/// combining timing, progress, and rate metrics. Replaces <c>TimingTree</c>.
/// </summary>
public sealed class OperationTree
{
    private readonly OperationTreeNode _root;
    private readonly TimeProvider _timeProvider;
    private readonly int _rpmWindowSeconds;

    public OperationTree(TimeProvider timeProvider, int rpmWindowSeconds = 60, string rootName = "total")
    {
        _timeProvider = timeProvider;
        _rpmWindowSeconds = rpmWindowSeconds;
        _root = new OperationTreeNode(rootName)
        {
            Status = OperationStatuses.InProgress,
            StartedAtUtc = timeProvider.GetUtcNow().UtcDateTime
        };
    }

    /// <summary>
    /// Creates a top-level child scope under the root.
    /// </summary>
    public OperationScope CreateScope(string name)
    {
        var child = new OperationTreeNode(name);
        lock (_root.Lock)
        {
            _root.Children.Add(child);
        }

        return new OperationScope(child, _timeProvider, _rpmWindowSeconds);
    }

    /// <summary>
    /// Backwards-compatible path-based timing, like <c>TimingTree.Track</c>.
    /// </summary>
    public void Track(string path, long elapsedMs)
    {
        var segments = path.Split('/');
        lock (_root.Lock)
        {
            TrackSegments(_root, segments, 0, elapsedMs);
        }
    }

    /// <summary>
    /// Produces an immutable deep-copy snapshot of the entire operation tree.
    /// </summary>
    public OperationNode Snapshot()
    {
        lock (_root.Lock)
        {
            return _root.CaptureSnapshot(_timeProvider);
        }
    }

    /// <summary>
    /// Marks the root as completed.
    /// </summary>
    public void Complete() => Finalize(OperationStatuses.Completed);

    /// <summary>
    /// Marks the root as failed.
    /// </summary>
    public void Fail() => Finalize(OperationStatuses.Failed);

    /// <summary>
    /// Marks the root as cancelled.
    /// </summary>
    public void Cancel() => Finalize(OperationStatuses.Cancelled);

    /// <summary>
    /// Marks the root with the given terminal status and records elapsed time.
    /// </summary>
    public void Finalize(string status)
    {
        var now = _timeProvider.GetUtcNow().UtcDateTime;
        lock (_root.Lock)
        {
            _root.Status = status;
            if (_root.StartedAtUtc.HasValue)
            {
                _root.ElapsedMs += (long)(now - _root.StartedAtUtc.Value).TotalMilliseconds;
                _root.StartedAtUtc = null;
            }
        }
    }

    private static void TrackSegments(OperationTreeNode parent, string[] segments, int index, long elapsedMs)
    {
        OperationTreeNode.TrackSegments(parent, segments, index, elapsedMs, markLeafComplete: false);
    }
}
