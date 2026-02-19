using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Issues.Command.Requests;

/// <summary>
/// Command to deactivate all issues that were not touched by the specified operation.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Command record - no logic to test.")]
public record DeactivateStaleIssuesCommand(string OperationId);
