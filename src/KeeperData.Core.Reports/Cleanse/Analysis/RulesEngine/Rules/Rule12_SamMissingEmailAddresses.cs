using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules;

/// <summary>
/// RULE 12, PRIORITY 3. Email addresses held in CTS but missing from SAM, where SAM holds none at all.
/// </summary>
/// <remarks>
/// Mutually exclusive with <see cref="Rule06_EmailsInconsistent"/>, which covers the case
/// where SAM holds some emails but not all of them. The exclusion is stated by the
/// preconditions of both rules rather than by an if and else pairing.
/// </remarks>
public sealed class Rule12_SamMissingEmailAddresses : ICleanseRule
{
    /// <inheritdoc />
    public RuleDescriptor Descriptor => RuleDescriptors.SamMissingEmailAddresses;

    /// <inheritdoc />
    public int Priority => 3;

    /// <inheritdoc />
    public AnalysisPass Pass => AnalysisPass.CtsPrimary;

    /// <inheritdoc />
    public RuleStatus Status => RuleStatus.Active;

    /// <inheritdoc />
    public bool AppliesTo(CtsSamRuleContext context)
        => context.ExistsInBoth
        && context.SamEmails.Length == 0
        && context.EmailsMissingFromSam.Length > 0;

    /// <inheritdoc />
    public RuleResult? Evaluate(CtsSamRuleContext context)
        => context.Issue(Descriptor, x => x.EmailCTS = context.EmailsMissingFromSam);
}
