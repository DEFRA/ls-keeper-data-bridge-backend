using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules;

/// <summary>
/// RULE 11, PRIORITY 5. Phone numbers held in CTS but missing from SAM, where SAM holds none at all.
/// </summary>
/// <remarks>
/// Mutually exclusive with <see cref="Rule09_PhonesInconsistent"/>.
/// </remarks>
public sealed class Rule11_SamMissingPhoneNumbers : ICleanseRule
{
    /// <inheritdoc />
    public RuleDescriptor Descriptor => RuleDescriptors.SamMissingPhoneNumbers;

    /// <inheritdoc />
    public int Priority => 5;

    /// <inheritdoc />
    public AnalysisPass Pass => AnalysisPass.CtsPrimary;

    /// <inheritdoc />
    public RuleStatus Status => RuleStatus.Active;

    /// <inheritdoc />
    public bool AppliesTo(CtsSamRuleContext context)
        => context.ExistsInBoth
        && context.SamPhones.Length == 0
        && context.PhonesMissingFromSam.Length > 0;

    /// <inheritdoc />
    public RuleResult? Evaluate(CtsSamRuleContext context)
        => context.Issue(Descriptor, x => x.TelCTS = context.PhonesMissingFromSam);
}
