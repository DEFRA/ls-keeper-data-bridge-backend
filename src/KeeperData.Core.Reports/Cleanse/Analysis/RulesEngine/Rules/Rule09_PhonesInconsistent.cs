using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules;

/// <summary>
/// RULE 9, PRIORITY 8. SAM holds phone numbers, but not all of those held in CTS.
/// </summary>
/// <remarks>
/// Mutually exclusive with <see cref="Rule11_SamMissingPhoneNumbers"/>.
/// The existing code comments label this rule as 9 in one place and 7 in another. The
/// descriptor in <see cref="RuleDescriptors.CtsSamPhoneNosInconsistent"/> states 9, and that
/// is what reaches the report, so 9 is used here.
/// </remarks>
public sealed class Rule09_PhonesInconsistent : ICleanseRule
{
    /// <inheritdoc />
    public RuleDescriptor Descriptor => RuleDescriptors.CtsSamPhoneNosInconsistent;

    /// <inheritdoc />
    public int Priority => 8;

    /// <inheritdoc />
    public AnalysisPass Pass => AnalysisPass.CtsPrimary;

    /// <inheritdoc />
    public RuleStatus Status => RuleStatus.Active;

    /// <inheritdoc />
    public bool AppliesTo(CtsSamRuleContext context)
        => context.ExistsInBoth
        && context.SamPhones.Length > 0
        && context.PhonesMissingFromSam.Length > 0;

    /// <inheritdoc />
    public RuleResult? Evaluate(CtsSamRuleContext context)
        => context.Issue(Descriptor, x =>
        {
            x.TelCTS = context.PhonesMissingFromSam;
            x.TelSAM = string.Join("; ", context.SamPhones);
        });
}
