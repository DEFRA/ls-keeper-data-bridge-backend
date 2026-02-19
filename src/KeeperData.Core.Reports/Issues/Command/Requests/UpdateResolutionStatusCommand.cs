using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Issues.Command.AggregateRoots;

namespace KeeperData.Core.Reports.Issues.Command.Requests;

/// <summary>
/// Command to update the resolution workflow status of an issue.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Command record - no logic to test.")]
public record UpdateResolutionStatusCommand(string IssueId, ResolutionStatus Status, string PerformedBy);
