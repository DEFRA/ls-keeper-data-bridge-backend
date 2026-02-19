using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Issues.Command.Requests;

/// <summary>
/// Command to record an issue when a rule condition is true.
/// </summary>
public record RecordIssueCommand(
    string OperationId,
    string Thumbprint,
    RuleDescriptor Descriptor,
    Cph Cph,
    string? CtsLidFullIdentifier = null,
    IssueContextData? IssueContext = null);
