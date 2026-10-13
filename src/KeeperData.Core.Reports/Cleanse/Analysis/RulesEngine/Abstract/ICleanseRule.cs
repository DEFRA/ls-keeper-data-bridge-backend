using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;

/// <summary>
/// A single, self contained cleanse analysis rule.
/// </summary>
/// <remarks>
/// Everything needed to understand a rule lives in the file that implements this interface:
/// its identity, its priority, the pass it runs in, its preconditions, and the check itself.
/// </remarks>
public interface ICleanseRule
{
    /// <summary>
    /// Gets the identity and reporting metadata for this rule.
    /// </summary>
    RuleDescriptor Descriptor { get; }

    /// <summary>
    /// Gets the execution priority. Lower values are evaluated first.
    /// Mirrors the documented PRIORITY ordering of the cleanse rule set.
    /// </summary>
    int Priority { get; }

    /// <summary>
    /// Gets the analysis pass during which this rule is evaluated.
    /// </summary>
    AnalysisPass Pass { get; }

    /// <summary>
    /// Gets the lifecycle state of this rule. Only active rules are evaluated.
    /// </summary>
    RuleStatus Status { get; }

    /// <summary>
    /// Determines whether the rule has anything to say about this record.
    /// </summary>
    /// <remarks>
    /// Preconditions only. Returning false means the rule is silent for this record.
    /// Keeping preconditions here, rather than inside the check, is what makes mutual
    /// exclusion between rules explicit instead of an artefact of control flow.
    /// </remarks>
    bool AppliesTo(CtsSamRuleContext context);

    /// <summary>
    /// Evaluates the rule against a record for which <see cref="AppliesTo"/> returned true.
    /// </summary>
    /// <returns>A result describing the issue, or null when no issue was detected.</returns>
    RuleResult? Evaluate(CtsSamRuleContext context);
}
