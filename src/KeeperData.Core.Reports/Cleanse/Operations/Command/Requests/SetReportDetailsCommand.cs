namespace KeeperData.Core.Reports.Cleanse.Operations.Command.Requests;

public record SetReportDetailsCommand(
    string OperationId,
    string ObjectKey,
    string ReportUrl);
