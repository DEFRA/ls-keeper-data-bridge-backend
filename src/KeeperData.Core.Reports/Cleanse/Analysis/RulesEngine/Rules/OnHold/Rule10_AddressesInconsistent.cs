using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules.OnHold;

/// <summary>
/// RULE 10, PRIORITY 9. Correspondence address details inconsistent between CTS and SAM. On hold.
/// </summary>
public sealed class Rule10_AddressesInconsistent : OnHoldCleanseRule
{
    /// <inheritdoc />
    public override RuleDescriptor Descriptor { get; } = new(
        RuleIds.CTS_SAM_INCONSISTENT_ADDRESSES, "10", "10",
        "Correspondence address details inconsistent between CTS and SAM", "TBC");

    /// <inheritdoc />
    public override int Priority => 9;

    /// <inheritdoc />
    public override AnalysisPass Pass => AnalysisPass.CtsPrimary;

    /// <inheritdoc />
    public override string HoldReason =>
        "Address comparison semantics have not yet been agreed with the business.";
}
