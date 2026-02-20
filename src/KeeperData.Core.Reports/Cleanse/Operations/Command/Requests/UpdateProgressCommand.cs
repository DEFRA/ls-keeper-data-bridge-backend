using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Cleanse.Operations.Command.Requests;

[ExcludeFromCodeCoverage(Justification = "Command record - no logic to test.")]
public record UpdateProgressCommand(
    string OperationId,
    double ProgressPercentage,
    string StatusDescription,
    int RecordsAnalyzed,
    int IssuesFound,
    int IssuesResolved);
