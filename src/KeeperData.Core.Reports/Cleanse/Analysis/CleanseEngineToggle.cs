using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reports.Cleanse.Analysis;

/// <summary>
/// Selects which cleanse analysis engine is registered.
/// </summary>
/// <remarks>
/// Deliberately a compile time constant rather than configuration, so that the choice is
/// visible in source control and cannot vary between environments by accident.
/// <para>
/// Set to true to use the rule based engine. Set to false to use the existing engine.
/// </para>
/// </remarks>
[ExcludeFromCodeCoverage(Justification = "Compile time constant - no logic to test.")]
public static class CleanseEngineToggle
{
    /// <summary>
    /// When true, <c>RuleBasedCleanseAnalysisEngine</c> is registered.
    /// When false, the existing <c>CleanseAnalysisEngine</c> is registered.
    /// </summary>
    /// <remarks>
    /// Declared as static readonly rather than const so that branching on it does not raise
    /// CS0162 unreachable code, which this solution treats as a build error.
    /// </remarks>
    public static readonly bool UseRuleBasedEngine = false;
}
