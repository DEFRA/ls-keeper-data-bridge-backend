using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Registry;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Service;

/// <summary>
/// Default rule execution service. Reads the rules to run from the registry.
/// </summary>
public sealed class CleanseRuleService(CleanseRuleRegistry ruleRegistry, ILogger<CleanseRuleService> logger) : ICleanseRuleService
{
    /// <inheritdoc />
    public List<RuleResult> Evaluate(CtsSamRuleContext context)
    {
        var results = new List<RuleResult>();

        foreach (var rule in ruleRegistry.For(context.Pass))
        {
            var result = EvaluateRule(rule, context);

            if (result is not null)
            {
                results.Add(result);
            }
        }

        return results;
    }

    /// <summary>
    /// Evaluates a single rule, isolating a failure in one rule from the rest of the run.
    /// </summary>
    /// <remarks>
    /// A rule that throws on malformed source data must not abandon the other rules for that
    /// record, nor stop the analysis. The failure is logged and treated as no issue found.
    /// </remarks>
    private RuleResult? EvaluateRule(ICleanseRule rule, CtsSamRuleContext context)
    {
        try
        {
            return rule.AppliesTo(context) ? rule.Evaluate(context) : null;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Cleanse rule {RuleNumber} ({RuleId}) failed for CPH {Cph}. Skipping this rule for this record.",
                rule.Descriptor.UserRuleNo, rule.Descriptor.RuleId, context.Cph.Value);

            return null;
        }
    }
}
