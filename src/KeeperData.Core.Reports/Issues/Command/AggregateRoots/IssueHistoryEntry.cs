namespace KeeperData.Core.Reports.Issues.Command.AggregateRoots;

/// <summary>
/// A single history/lineage entry recording an action taken on an issue.
/// Stored separately from the Issue aggregate root.
/// </summary>
public class IssueHistoryEntry
{
    /// <summary>
    /// Unique identifier for this history entry.
    /// </summary>
    public required string Id { get; set; }

    /// <summary>
    /// The issue this entry relates to.
    /// </summary>
    public required string IssueId { get; set; }

    /// <summary>
    /// The type of action that was performed.
    /// </summary>
    public IssueAction Action { get; set; }

    /// <summary>
    /// Who performed the action (user id/email, or "system" for automated actions).
    /// </summary>
    public string PerformedBy { get; set; } = "system";

    /// <summary>
    /// Human-readable detail about the change (e.g. "ResolutionStatus: None → InProgress").
    /// </summary>
    public string? Detail { get; set; }

    /// <summary>
    /// When the action occurred.
    /// </summary>
    public DateTime OccurredAtUtc { get; set; }

    /// <summary>
    /// Creates a new history entry.
    /// </summary>
    public static IssueHistoryEntry Create(string issueId, IssueAction action, string performedBy, string? detail = null) => new()
    {
        Id = Guid.NewGuid().ToString(),
        IssueId = issueId,
        Action = action,
        PerformedBy = performedBy,
        Detail = detail,
        OccurredAtUtc = DateTime.UtcNow
    };
}
