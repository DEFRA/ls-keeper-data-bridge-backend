namespace KeeperData.Core.Reports.Cleanse.Operations.Command.Requests;

public record UpdateReportUrlCommand(
    string OperationId,
    string ReportUrl);
