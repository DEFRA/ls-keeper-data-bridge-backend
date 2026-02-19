using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Cleanse.Operations.Command.Requests;

/// <summary>
/// Command to create a new cleanse analysis operation.
/// </summary>
/// <param name="TotalRecords">The total number of records to be analyzed (0 if unknown upfront).</param>
[ExcludeFromCodeCoverage(Justification = "Command record - no logic to test.")]
public record CreateOperationCommand(int TotalRecords = 0);
