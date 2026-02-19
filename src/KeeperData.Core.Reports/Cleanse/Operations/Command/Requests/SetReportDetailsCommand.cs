using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Cleanse.Operations.Command.Requests;

[ExcludeFromCodeCoverage(Justification = "Command record - no logic to test.")]
public record SetReportDetailsCommand(
    string OperationId,
    string ObjectKey,
    string ReportUrl);
