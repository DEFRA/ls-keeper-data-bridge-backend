using KeeperData.Core.Reports.Issues.Query.Dtos;

namespace KeeperData.Core.Reports.Issues.Query.Abstract;

public interface IIssueQueries
{
    /// <summary>
    /// Gets the total count of active issues.
    /// </summary>
    Task<long> GetActiveIssuesCountAsync(CancellationToken ct = default);

    /// <summary>
    /// Gets a cleanse report item by its identifier.
    /// </summary>
    Task<IssueDto?> GetByIdAsync(string id, CancellationToken ct = default);

    /// <summary>
    /// Gets active issues with pagination.
    /// </summary>
    Task<IReadOnlyList<IssueDto>> GetActiveIssuesAsync(int skip, int top, CancellationToken ct = default);

    /// <summary>
    /// Streams all active issues in batches. Memory-efficient for large datasets.
    /// </summary>
    /// <param name="batchSize">Number of items to fetch per batch.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An async enumerable of active issues.</returns>
    IAsyncEnumerable<IssueDto> StreamActiveIssuesAsync(int batchSize = 1000, CancellationToken ct = default);

    /// <summary>
    /// Streams all active issues ordered by rule priority then by CPH within each group.
    /// Memory-efficient for CSV export with deterministic ordering.
    /// </summary>
    /// <param name="rulePriorityOrder">Issue codes in priority order.</param>
    /// <param name="batchSize">Number of items to fetch per batch.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An async enumerable of active issues ordered by rule priority then CPH.</returns>
    IAsyncEnumerable<IssueDto> StreamActiveIssuesByRulePriorityAsync(
        IReadOnlyList<string> rulePriorityOrder, int batchSize = 1000, CancellationToken ct = default);

    Task<CleanseIssueQueryResultDto> QueryAsync(CleanseIssueQueryDto query, CancellationToken ct = default);
    Task<int> CountAsync(CleanseIssueQueryDto query, CancellationToken ct = default);
    Task<IReadOnlyList<CleanseIssueGroupResultDto>> GroupByIssueCodeAsync(CleanseIssueQueryDto? baseFilter = null, int itemsPerGroup = 10, CancellationToken ct = default);
    Task<CleanseIssuesResultDto> ListIssuesAsync(int skip = 0, int top = 50, CancellationToken ct = default);

    /// <summary>
    /// Gets paginated history/lineage entries for a specific issue.
    /// </summary>
    Task<IReadOnlyList<IssueHistoryEntryDto>> GetIssueHistoryAsync(string issueId, int skip = 0, int top = 50, CancellationToken ct = default);

    /// <summary>
    /// Gets the total count of history entries for a specific issue.
    /// </summary>
    Task<long> GetIssueHistoryCountAsync(string issueId, CancellationToken ct = default);
}
