using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules;

/// <summary>
/// RULE 2A, PRIORITY 1. Active CTS CPH inactive or missing in SAM.
/// </summary>
/// <remarks>
/// Terminal for the CTS primary pass. When this rule fires there is no SAM holding to
/// compare against, so no comparison rule can say anything useful. That is expressed by
/// every other CTS pass rule requiring <see cref="CtsSamRuleContext.ExistsInBoth"/>,
/// rather than by an early return inside the engine.
/// </remarks>
public sealed class Rule02A_CtsCphNotInSam : ICleanseRule
{
    /// <inheritdoc />
    public RuleDescriptor Descriptor => RuleDescriptors.CtsCphNotInSam;

    /// <inheritdoc />
    public int Priority => 1;

    /// <inheritdoc />
    public AnalysisPass Pass => AnalysisPass.CtsPrimary;

    /// <inheritdoc />
    public RuleStatus Status => RuleStatus.Active;

    /// <inheritdoc />
    public bool AppliesTo(CtsSamRuleContext context) => true;

    /// <inheritdoc />
    public RuleResult? Evaluate(CtsSamRuleContext context)
        => context.ExistsInSam ? null : context.Issue(Descriptor);
}
