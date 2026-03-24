namespace KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;

/// <summary>
/// Mutable accumulator that builds a hierarchical tree of timing data.
/// Uses a path-based API where "/" separates parent and child segments,
/// e.g. <c>Track("CTS Pump/fetching", 150)</c> creates a "CTS Pump" parent
/// with a "fetching" child and adds 150 ms to the child.
/// </summary>
public sealed class TimingTree
{
    private readonly Dictionary<string, long> _leafTotals = new(StringComparer.Ordinal);
    private readonly object _lock = new();

    /// <summary>
    /// Adds elapsed milliseconds to the leaf node identified by <paramref name="path"/>.
    /// Intermediate nodes are created automatically.
    /// Thread-safe for concurrent pump instrumentation.
    /// </summary>
    /// <param name="path">Slash-separated path, e.g. "CTS Pump/fetching".</param>
    /// <param name="elapsedMs">Milliseconds to add to this leaf.</param>
    public void Track(string path, long elapsedMs)
    {
        lock (_lock)
        {
            _leafTotals.TryGetValue(path, out var current);
            _leafTotals[path] = current + elapsedMs;
        }
    }

    /// <summary>
    /// Produces a deep-copy <see cref="TimingNode"/> snapshot of the current timing data.
    /// Parent node elapsed values are computed as the sum of their children.
    /// </summary>
    /// <param name="rootName">Name for the root node (default "total").</param>
    public TimingNode Snapshot(string rootName = "total")
    {
        Dictionary<string, long> snapshot;
        lock (_lock)
        {
            snapshot = new Dictionary<string, long>(_leafTotals, StringComparer.Ordinal);
        }

        var root = new TimingNode { Name = rootName };

        if (snapshot.Count == 0)
            return root;

        EnsureChildren(root);

        foreach (var (path, ms) in snapshot)
        {
            var segments = path.Split('/');
            var current = root;

            for (var i = 0; i < segments.Length; i++)
            {
                var name = segments[i];
                EnsureChildren(current);

                var child = current.Children!.Find(c => c.Name == name);
                if (child is null)
                {
                    child = new TimingNode { Name = name };
                    current.Children!.Add(child);
                }

                if (i == segments.Length - 1)
                {
                    // Leaf — set the tracked value directly
                    child.ElapsedMs = ms;
                    child.Elapsed = TimingNode.FormatElapsed(ms);
                }

                current = child;
            }
        }

        // Roll up parent totals from children
        RollUp(root);
        return root;
    }

    /// <summary>
    /// Merges all entries from <paramref name="other"/> under the given <paramref name="prefix"/>.
    /// </summary>
    public void Merge(TimingTree other, string prefix)
    {
        Dictionary<string, long> otherSnapshot;
        lock (other._lock)
        {
            otherSnapshot = new Dictionary<string, long>(other._leafTotals, StringComparer.Ordinal);
        }

        lock (_lock)
        {
            foreach (var (path, ms) in otherSnapshot)
            {
                var fullPath = string.IsNullOrEmpty(prefix) ? path : $"{prefix}/{path}";
                _leafTotals.TryGetValue(fullPath, out var current);
                _leafTotals[fullPath] = current + ms;
            }
        }
    }

    /// <summary>
    /// Recursively sums child elapsed values into parent nodes.
    /// </summary>
    private static void RollUp(TimingNode node)
    {
        if (node.Children is null or { Count: 0 })
            return;

        foreach (var child in node.Children)
        {
            RollUp(child);
        }

        node.ElapsedMs = node.Children.Sum(c => c.ElapsedMs);
        node.Elapsed = TimingNode.FormatElapsed(node.ElapsedMs);
    }

    private static void EnsureChildren(TimingNode node)
    {
        node.Children ??= [];
    }
}
