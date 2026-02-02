using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Querying;

/// <summary>
/// Result of a cleanse issue query with pagination metadata.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public sealed record CleanseIssueQueryResult
{
    /// <summary>The issues matching the query.</summary>
    public required IReadOnlyList<CleanseReportItem> Items { get; init; }

    /// <summary>Total count of matching issues (before pagination).</summary>
    public required int TotalCount { get; init; }

    /// <summary>Number of items skipped.</summary>
    public required int Skip { get; init; }

    /// <summary>Maximum items requested.</summary>
    public required int Top { get; init; }

    /// <summary>Whether there are more items available.</summary>
    public bool HasMore => Skip + Items.Count < TotalCount;
}
