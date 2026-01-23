namespace KeeperData.Core.Reports.Querying;

/// <summary>
/// Service for querying cleanse issues with filtering, sorting, and grouping.
/// </summary>
public interface ICleanseIssueQueryService
{
    /// <summary>
    /// Queries issues with filtering, sorting, and pagination.
    /// </summary>
    /// <param name="query">The query parameters.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Query result with items and pagination metadata.</returns>
    Task<CleanseIssueQueryResult> QueryAsync(CleanseIssueQuery query, CancellationToken ct = default);

    /// <summary>
    /// Gets a count of issues matching the query.
    /// </summary>
    /// <param name="query">The query parameters.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Count of matching issues.</returns>
    Task<int> CountAsync(CleanseIssueQuery query, CancellationToken ct = default);

    /// <summary>
    /// Groups issues by issue code with optional filtering.
    /// </summary>
    /// <param name="baseFilter">Optional base filter to apply before grouping.</param>
    /// <param name="itemsPerGroup">Maximum number of items to return per group.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>List of groups with counts and sample items.</returns>
    Task<IReadOnlyList<CleanseIssueGroupResult>> GroupByIssueCodeAsync(
        CleanseIssueQuery? baseFilter = null,
        int itemsPerGroup = 10,
        CancellationToken ct = default);
}
