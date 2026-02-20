using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Issues.Command.Requests;

/// <summary>
/// Command to clear the assigned user from an issue.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Command record - no logic to test.")]
public record UnassignIssueCommand(string IssueId, string PerformedBy);
