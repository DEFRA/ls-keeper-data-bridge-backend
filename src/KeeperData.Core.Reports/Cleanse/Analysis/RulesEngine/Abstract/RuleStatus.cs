namespace KeeperData.Core.Reports.Cleanse.Analysis.RulesEngine.Abstract;

/// <summary>
/// Lifecycle state of a rule.
/// </summary>
/// <remarks>
/// Rules that are specified but not currently running stay registered with a status of
/// <see cref="OnHold"/> so that they remain visible and documented, instead of existing
/// only as an unused constant.
/// </remarks>
public enum RuleStatus
{
    /// <summary>
    /// The rule is evaluated during analysis.
    /// </summary>
    Active,

    /// <summary>
    /// The rule is specified but deliberately not evaluated. A reason must be stated on the rule.
    /// </summary>
    OnHold,

    /// <summary>
    /// The rule has been identified but no implementation exists yet.
    /// </summary>
    NotImplemented
}
