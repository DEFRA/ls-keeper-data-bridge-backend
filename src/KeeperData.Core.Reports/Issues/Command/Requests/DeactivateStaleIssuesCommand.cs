namespace KeeperData.Core.Reports.Issues.Command.Requests;

/// <summary>
/// Command to deactivate all issues that were not touched by the specified operation.
/// </summary>
public record DeactivateStaleIssuesCommand(string OperationId);
