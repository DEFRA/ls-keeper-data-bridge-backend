using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Issues.Command.Requests;

/// <summary>
/// Command to flag an issue as ignored.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Command record - no logic to test.")]
public record IgnoreIssueCommand(string IssueId, string PerformedBy);
