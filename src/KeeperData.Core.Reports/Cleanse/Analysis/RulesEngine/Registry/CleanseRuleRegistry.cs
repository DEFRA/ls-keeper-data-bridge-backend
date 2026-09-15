using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Context;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules;
using KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Rules.OnHold;

namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Registry;

/// <summary>
/// The rule book. The single place that answers which cleanse rules exist, in what order
/// they run, which pass they belong to, and which are currently switched off.
/// </summary>
public sealed class CleanseRuleRegistry
{
    private readonly IReadOnlyList<ICleanseRule> _all;
    private readonly IReadOnlyList<ICleanseRule> _ctsPrimaryRules;
    private readonly IReadOnlyList<ICleanseRule> _samPrimaryRules;

    /// <summary>
    /// Initialises the registry, ordering rules by priority.
    /// </summary>
    /// <param name="rules">The rules to register. Defaults to <see cref="CreateDefaultRules"/>.</param>
    public CleanseRuleRegistry(IEnumerable<ICleanseRule>? rules = null)
    {
        _all =
        [
            .. (rules ?? CreateDefaultRules())
                .OrderBy(rule => rule.Priority)
                .ThenBy(rule => rule.Descriptor.UserRuleNo, StringComparer.Ordinal)
        ];

        var activeRules = _all.Where(rule => rule.Status == RuleStatus.Active).ToList();

        _ctsPrimaryRules = [.. activeRules.Where(rule => rule.Pass == AnalysisPass.CtsPrimary)];
        _samPrimaryRules = [.. activeRules.Where(rule => rule.Pass == AnalysisPass.SamPrimary)];
    }

    /// <summary>
    /// Every rule known to the system, active or otherwise, in execution order.
    /// </summary>
    public static IEnumerable<ICleanseRule> CreateDefaultRules() =>
    [
        new Rule01_SamNoCattleUnit(),
        new Rule02A_CtsCphNotInSam(),
        new Rule02B_SamCphNotInCts(),
        new Rule03_CtsSamLocationsDiffer(),
        new Rule04_NoEmailInEitherSystem(),
        new Rule05_NoPhoneInEitherSystem(),
        new Rule06_EmailsInconsistent(),
        new Rule09_PhonesInconsistent(),
        new Rule11_SamMissingPhoneNumbers(),
        new Rule12_SamMissingEmailAddresses(),

        new Rule07_SamMissingFsaNumber(),
        new Rule08_CtsCattleCphMissing(),
        new Rule10_AddressesInconsistent(),
    ];

    /// <summary>
    /// Gets every registered rule, in execution order, including those on hold.
    /// </summary>
    public IReadOnlyList<ICleanseRule> All => _all;

    /// <summary>
    /// Gets the active rules for the given pass, in execution order.
    /// </summary>
    public IReadOnlyList<ICleanseRule> For(AnalysisPass pass)
        => pass == AnalysisPass.CtsPrimary ? _ctsPrimaryRules : _samPrimaryRules;

    /// <summary>
    /// Finds a rule by its business facing rule number, for example "2A".
    /// </summary>
    public ICleanseRule? ByNumber(string ruleNumber)
        => _all.FirstOrDefault(rule => string.Equals(rule.Descriptor.UserRuleNo, ruleNumber, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// Produces a human readable dump of the rule book, suitable for a diagnostics endpoint
    /// or for review by the business.
    /// </summary>
    public string Describe()
        => string.Join(Environment.NewLine, _all.Select(rule =>
            $"P{rule.Priority,-3} Rule {rule.Descriptor.UserRuleNo,-3} {rule.Status,-14} {rule.Pass,-10} {rule.Descriptor.RuleId,-38} {rule.Descriptor.UserDescription}"));
}
