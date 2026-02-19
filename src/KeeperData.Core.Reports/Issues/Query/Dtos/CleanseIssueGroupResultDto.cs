using KeeperData.Core.Reports.Issues.Command.AggregateRoots;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Issues.Query.Dtos;

/// <summary>
/// Result of grouping issues by issue code.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public sealed record CleanseIssueGroupResultDto
{
    /// <summary>The issue code for this group.</summary>
    public required string IssueCode { get; init; }

    /// <summary>Total count of issues in this group.</summary>
    public required int TotalCount { get; init; }

    /// <summary>Sample of issues in this group (limited by itemsPerGroup).</summary>
    public required IReadOnlyList<IssueDto> Items { get; init; }
}
