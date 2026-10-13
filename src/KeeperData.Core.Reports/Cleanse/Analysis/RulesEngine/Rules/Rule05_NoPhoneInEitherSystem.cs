using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules;

/// <summary>
/// RULE 5, PRIORITY 4. No telephone numbers in CTS and SAM.
/// </summary>
public sealed class Rule05_NoPhoneInEitherSystem : ICleanseRule
{
    /// <inheritdoc />
    public RuleDescriptor Descriptor => RuleDescriptors.CtsSamNoPhoneNumbers;

    /// <inheritdoc />
    public int Priority => 4;

    /// <inheritdoc />
    public AnalysisPass Pass => AnalysisPass.CtsPrimary;

    /// <inheritdoc />
    public RuleStatus Status => RuleStatus.Active;

    /// <inheritdoc />
    public bool AppliesTo(CtsSamRuleContext context) => context.ExistsInBoth;

    /// <inheritdoc />
    public RuleResult? Evaluate(CtsSamRuleContext context)
        => context.CtsPhones.Length + context.SamPhones.Length == 0
            ? context.Issue(Descriptor)
            : null;
}
