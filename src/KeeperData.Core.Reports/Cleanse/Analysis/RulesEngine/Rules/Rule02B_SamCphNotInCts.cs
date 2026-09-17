using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules;

/// <summary>
/// RULE 2B, PRIORITY 1. Active SAM CPH inactive or missing in CTS.
/// </summary>
/// <remarks>
/// The mirror of <see cref="Rule02A_CtsCphNotInSam"/>, declared as its own rule on the SAM
/// primary pass so that the pairing is visible from the rule set rather than only from
/// reading a second processing method.
/// </remarks>
public sealed class Rule02B_SamCphNotInCts : ICleanseRule
{
    /// <inheritdoc />
    public RuleDescriptor Descriptor => RuleDescriptors.SamCphNotInCts;

    /// <inheritdoc />
    public int Priority => 1;

    /// <inheritdoc />
    public AnalysisPass Pass => AnalysisPass.SamPrimary;

    /// <inheritdoc />
    public RuleStatus Status => RuleStatus.Active;

    /// <inheritdoc />
    public bool AppliesTo(CtsSamRuleContext context) => true;

    /// <inheritdoc />
    public RuleResult? Evaluate(CtsSamRuleContext context)
        => context.ExistsInCts ? null : context.Issue(Descriptor);
}
