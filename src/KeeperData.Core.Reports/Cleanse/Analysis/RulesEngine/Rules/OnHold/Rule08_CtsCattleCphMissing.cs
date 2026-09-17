using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules.OnHold;

/// <summary>
/// RULE 8, PRIORITY 12. Cattle related CPHs missing in CTS. On hold.
/// </summary>
public sealed class Rule08_CtsCattleCphMissing : OnHoldCleanseRule
{
    /// <inheritdoc />
    public override RuleDescriptor Descriptor { get; } = new(
        RuleIds.CTS_CATTLE_CPH_MISSING, "8", "08", "Cattle-related CPHs missing in CTS", "TBC");

    /// <inheritdoc />
    public override int Priority => 12;

    /// <inheritdoc />
    public override AnalysisPass Pass => AnalysisPass.SamPrimary;

    /// <inheritdoc />
    public override string HoldReason =>
        "Virtually identical to rule 2B. Would produce duplicate issues until the "
        + "cattle specific narrowing is agreed with the business.";
}
