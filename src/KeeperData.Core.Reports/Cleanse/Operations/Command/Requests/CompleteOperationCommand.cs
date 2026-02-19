namespace KeeperData.Core.Reports.Cleanse.Operations.Command.Requests;

public record CompleteOperationCommand(
    string OperationId,
    int RecordsAnalyzed,
    int IssuesFound,
    int IssuesResolved,
    long DurationMs);
