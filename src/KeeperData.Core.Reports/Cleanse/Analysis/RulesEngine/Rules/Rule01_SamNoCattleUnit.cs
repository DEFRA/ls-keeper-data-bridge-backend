using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;
using KeeperData.Core.Reports.Domain;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules;

/// <summary>
/// RULE 1, PRIORITY 6. No cattle unit defined in SAM.
/// </summary>
/// <remarks>
/// Applies where the CPH is present in both CTS and SAM. An issue is raised when the SAM
/// holding does not declare an animal species code of CTT.
/// </remarks>
public sealed class Rule01_SamNoCattleUnit : ICleanseRule
{
    /// <summary>
    /// The SAM animal species code denoting a cattle unit.
    /// </summary>
    public const string CattleSpeciesCode = "CTT";

    /// <inheritdoc />
    public RuleDescriptor Descriptor => RuleDescriptors.SamNoCattleUnit;

    /// <inheritdoc />
    public int Priority => 6;

    /// <inheritdoc />
    public AnalysisPass Pass => AnalysisPass.CtsPrimary;

    /// <inheritdoc />
    public RuleStatus Status => RuleStatus.Active;

    /// <inheritdoc />
    public bool AppliesTo(CtsSamRuleContext context) => context.ExistsInBoth;

    /// <inheritdoc />
    public RuleResult? Evaluate(CtsSamRuleContext context)
    {
        var animalSpeciesCode = context.Sam!.AnimalSpeciesCode;

        if (animalSpeciesCode == CattleSpeciesCode)
        {
            return null;
        }

        return context.Issue(Descriptor, x => x.AnimalSpeciesCode = animalSpeciesCode);
    }
}
