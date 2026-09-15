using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Service;

/// <summary>
/// Runs the registered cleanse rules against a single record.
/// </summary>
/// <remarks>
/// Separated from the rule registry so that the registry stays a catalogue and this stays
/// the place where per rule execution concerns live, such as fault isolation and logging.
/// </remarks>
public interface ICleanseRuleService
{
    /// <summary>
    /// Evaluates the active rules for the context's analysis pass, in priority order.
    /// </summary>
    /// <returns>The issues detected for this record. Empty when the record is clean.</returns>
    List<RuleResult> Evaluate(CtsSamRuleContext context);
}
