using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Dtos;

/// <summary>
/// Represents a paginated result of cleanse issues.
/// </summary>
public record CleanseIssuesResult
{
    /// <summary>
    /// Gets the list of cleanse issues.
    /// </summary>
    public required IReadOnlyList<CleanseReportItem> Items { get; init; }

    /// <summary>
    /// Gets the total count of issues matching the query.
    /// </summary>
    public int TotalCount { get; init; }

    /// <summary>
    /// Gets the number of records skipped.
    /// </summary>
    public int Skip { get; init; }

    /// <summary>
    /// Gets the maximum number of records requested.
    /// </summary>
    public int Top { get; init; }
}
