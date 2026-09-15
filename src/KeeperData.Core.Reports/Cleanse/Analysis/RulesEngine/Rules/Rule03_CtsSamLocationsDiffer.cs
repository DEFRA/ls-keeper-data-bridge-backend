using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules;

/// <summary>
/// RULE 3, PRIORITY 10. Cattle related CPH in SAM whose location name is unknown, blank,
/// or does not match the CTS location name.
/// </summary>
/// <remarks>
/// Location name is FEATURE_NAME in SAM and ADR_NAME in CTS.
/// This rule reuses the SAM_CATTLE_RELATED_CPHs rule id, because that is what the existing
/// engine records and what the business already sees in the report. Changing it is a
/// separate decision.
/// </remarks>
public sealed class Rule03_CtsSamLocationsDiffer : ICleanseRule
{
    private static readonly string[] UnknownLocationNames = ["unknown", "not known", "notknown"];

    /// <inheritdoc />
    public RuleDescriptor Descriptor => RuleDescriptors.CtsSamLocationsDiffer;

    /// <inheritdoc />
    public int Priority => 10;

    /// <inheritdoc />
    public AnalysisPass Pass => AnalysisPass.CtsPrimary;

    /// <inheritdoc />
    public RuleStatus Status => RuleStatus.Active;

    /// <inheritdoc />
    public bool AppliesTo(CtsSamRuleContext context)
        => context.ExistsInBoth
        && context.Sam!.AnimalSpeciesCode == Rule01_SamNoCattleUnit.CattleSpeciesCode;

    /// <inheritdoc />
    public RuleResult? Evaluate(CtsSamRuleContext context)
    {
        var samLocationName = context.Sam!.LocationName;
        var ctsLocationName = context.Cts!.LocationName;

        var mismatch = string.IsNullOrWhiteSpace(samLocationName)
            || UnknownLocationNames.Contains(samLocationName, StringComparer.OrdinalIgnoreCase)
            || !string.Equals(ctsLocationName, samLocationName, StringComparison.OrdinalIgnoreCase);

        if (!mismatch)
        {
            return null;
        }

        return context.Issue(Descriptor, x =>
        {
            x.LocationNameSAM = samLocationName;
            x.LocationNameCTS = ctsLocationName;
        });
    }
}
