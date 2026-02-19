namespace KeeperData.Core.Reports.Issues.Command.Requests;

/// <summary>
/// Command to assign an issue to a user.
/// </summary>
public record AssignIssueCommand(string IssueId, string AssignedTo, string PerformedBy);
