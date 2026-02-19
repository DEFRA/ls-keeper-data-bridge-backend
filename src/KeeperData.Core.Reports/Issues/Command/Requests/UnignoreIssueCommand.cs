using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Issues.Command.Requests;

/// <summary>
/// Command to remove the ignored flag from an issue.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Command record - no logic to test.")]
public record UnignoreIssueCommand(string IssueId, string PerformedBy);
