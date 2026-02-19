namespace KeeperData.Core.Reports.Issues.Command.Requests;

/// <summary>
/// Command to clear the assigned user from an issue.
/// </summary>
public record UnassignIssueCommand(string IssueId, string PerformedBy);
