using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules;

/// <summary>
/// RULE 6, PRIORITY 7. SAM holds email addresses, but not all of those held in CTS.
/// </summary>
/// <remarks>
/// Mutually exclusive with <see cref="Rule12_SamMissingEmailAddresses"/>, which covers the
/// case where SAM holds no email addresses at all.
/// </remarks>
public sealed class Rule06_EmailsInconsistent : ICleanseRule
{
    /// <inheritdoc />
    public RuleDescriptor Descriptor => RuleDescriptors.CtsSamEmailAddressesInconsistent;

    /// <inheritdoc />
    public int Priority => 7;

    /// <inheritdoc />
    public AnalysisPass Pass => AnalysisPass.CtsPrimary;

    /// <inheritdoc />
    public RuleStatus Status => RuleStatus.Active;

    /// <inheritdoc />
    public bool AppliesTo(CtsSamRuleContext context)
        => context.ExistsInBoth
        && context.SamEmails.Length > 0
        && context.EmailsMissingFromSam.Length > 0;

    /// <inheritdoc />
    public RuleResult? Evaluate(CtsSamRuleContext context)
        => context.Issue(Descriptor, x =>
        {
            x.EmailCTS = context.EmailsMissingFromSam;
            x.EmailSAM = string.Join("; ", context.SamEmails);
        });
}
