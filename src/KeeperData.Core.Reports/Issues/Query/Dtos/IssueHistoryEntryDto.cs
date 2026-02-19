using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Issues.Query.Dtos;

/// <summary>
/// Read-side DTO for an issue history/lineage entry.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO class - no logic to test.")]
public class IssueHistoryEntryDto
{
    public required string Id { get; set; }
    public required string IssueId { get; set; }
    public required string Action { get; set; }
    public string PerformedBy { get; set; } = "system";
    public string? Detail { get; set; }
    public DateTime OccurredAtUtc { get; set; }
}
