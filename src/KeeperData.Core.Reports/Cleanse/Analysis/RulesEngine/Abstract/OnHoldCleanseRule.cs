using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;

/// <summary>
/// Base class for rules that are specified but deliberately not evaluated.
/// </summary>
/// <remarks>
/// On hold rules stay registered and documented, with a stated reason, so that the question
/// "what happened to rule 10" is answered in code rather than by an unused constant.
/// They never apply and never raise an issue.
/// </remarks>
public abstract class OnHoldCleanseRule : ICleanseRule
{
    /// <inheritdoc />
    public abstract RuleDescriptor Descriptor { get; }

    /// <inheritdoc />
    public abstract int Priority { get; }

    /// <inheritdoc />
    public abstract AnalysisPass Pass { get; }

    /// <summary>
    /// Gets the reason this rule is not currently evaluated.
    /// </summary>
    public abstract string HoldReason { get; }

    /// <inheritdoc />
    public RuleStatus Status => RuleStatus.OnHold;

    /// <inheritdoc />
    public bool AppliesTo(CtsSamRuleContext context) => false;

    /// <inheritdoc />
    public RuleResult? Evaluate(CtsSamRuleContext context) => null;
}
