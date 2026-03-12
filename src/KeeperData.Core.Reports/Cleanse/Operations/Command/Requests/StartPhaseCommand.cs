using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;

namespace KeeperData.Core.Reports.Cleanse.Operations.Command.Requests;

[ExcludeFromCodeCoverage(Justification = "Command record - no logic to test.")]
public record StartPhaseCommand(
    string OperationId,
    OperationPhase Phase,
    int TotalRecords);
