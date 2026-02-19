using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Cleanse.Operations.Command.Requests;

[ExcludeFromCodeCoverage(Justification = "Command record - no logic to test.")]
public record CompleteOperationCommand(
    string OperationId,
    int RecordsAnalyzed,
    int IssuesFound,
    int IssuesResolved,
    long DurationMs);
