using KeeperData.Core.Reports.Issues.Command.AggregateRoots;

namespace KeeperData.Core.Reports.Issues.Command.Abstract;

/// <summary>
/// Write repository for persisting issue history/lineage entries (aggregate root side).
/// </summary>
public interface IIssueHistoryAggRootRepository
{
    /// <summary>
    /// Appends a single history entry.
    /// </summary>
    Task AppendAsync(IssueHistoryEntry entry, CancellationToken ct = default);

    /// <summary>
    /// Appends multiple history entries in a single batch.
    /// </summary>
    Task AppendBatchAsync(IReadOnlyList<IssueHistoryEntry> entries, CancellationToken ct = default);
}
