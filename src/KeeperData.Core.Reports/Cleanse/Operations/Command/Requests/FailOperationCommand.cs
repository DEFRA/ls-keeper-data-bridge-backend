namespace KeeperData.Core.Reports.Cleanse.Operations.Command.Requests;

public record FailOperationCommand(
    string OperationId,
    string Error,
    long DurationMs);
