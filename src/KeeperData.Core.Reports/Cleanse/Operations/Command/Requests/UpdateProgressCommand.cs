namespace KeeperData.Core.Reports.Cleanse.Operations.Command.Requests;

public record UpdateProgressCommand(
    string OperationId,
    double ProgressPercentage,
    string StatusDescription,
    int RecordsAnalyzed,
    int IssuesFound,
    int IssuesResolved);
