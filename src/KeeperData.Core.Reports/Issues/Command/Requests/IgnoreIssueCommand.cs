namespace KeeperData.Core.Reports.Issues.Command.Requests;

/// <summary>
/// Command to flag an issue as ignored.
/// </summary>
public record IgnoreIssueCommand(string IssueId, string PerformedBy);
