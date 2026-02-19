namespace KeeperData.Core.Reports.Issues.Command.Requests;

/// <summary>
/// Command to remove the ignored flag from an issue.
/// </summary>
public record UnignoreIssueCommand(string IssueId, string PerformedBy);
