using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules;

/// <summary>
/// RULE 4, PRIORITY 2. CPH present in both CTS and SAM but no email addresses in either system.
/// </summary>
public sealed class Rule04_NoEmailInEitherSystem : ICleanseRule
{
    /// <inheritdoc />
    public RuleDescriptor Descriptor => RuleDescriptors.CtsSamNoEmailAddresses;

    /// <inheritdoc />
    public int Priority => 2;

    /// <inheritdoc />
    public AnalysisPass Pass => AnalysisPass.CtsPrimary;

    /// <inheritdoc />
    public RuleStatus Status => RuleStatus.Active;

    /// <inheritdoc />
    public bool AppliesTo(CtsSamRuleContext context) => context.ExistsInBoth;

    /// <inheritdoc />
    public RuleResult? Evaluate(CtsSamRuleContext context)
        => context.CtsEmails.Length + context.SamEmails.Length == 0
            ? context.Issue(Descriptor)
            : null;
}
