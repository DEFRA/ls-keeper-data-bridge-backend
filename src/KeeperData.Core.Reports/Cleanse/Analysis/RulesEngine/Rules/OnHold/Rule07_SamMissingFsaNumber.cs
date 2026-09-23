using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules.OnHold;

/// <summary>
/// RULE 7, PRIORITY 11. CPHs in SAM missing an FSA number. On hold.
/// </summary>
public sealed class Rule07_SamMissingFsaNumber : OnHoldCleanseRule
{
    /// <inheritdoc />
    public override RuleDescriptor Descriptor { get; } = new(
        RuleIds.SAM_MISSING_FSA_NO, "7", "07", "CPHs in SAM missing FSA number", "TBC");

    /// <inheritdoc />
    public override int Priority => 11;

    /// <inheritdoc />
    public override AnalysisPass Pass => AnalysisPass.CtsPrimary;

    /// <inheritdoc />
    public override string HoldReason =>
        "CTS records with LTY_LOC_TYPE of SH use the identifier format SH-{FSANUMBER:d4}. "
        + "The matching SAM record cannot be identified from the logic provided.";
}
