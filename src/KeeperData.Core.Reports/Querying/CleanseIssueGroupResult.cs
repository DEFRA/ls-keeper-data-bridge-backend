using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Querying;

/// <summary>
/// Result of grouping issues by issue code.
/// </summary>
public sealed record CleanseIssueGroupResult
{
    /// <summary>The issue code for this group.</summary>
    public required string IssueCode { get; init; }

    /// <summary>Total count of issues in this group.</summary>
    public required int TotalCount { get; init; }

    /// <summary>Sample of issues in this group (limited by itemsPerGroup).</summary>
    public required IReadOnlyList<CleanseReportItem> Items { get; init; }
}
