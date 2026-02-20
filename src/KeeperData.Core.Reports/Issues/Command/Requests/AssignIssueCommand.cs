using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Issues.Command.Requests;

/// <summary>
/// Command to assign an issue to a user.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Command record - no logic to test.")]
public record AssignIssueCommand(string IssueId, string AssignedTo, string PerformedBy);
