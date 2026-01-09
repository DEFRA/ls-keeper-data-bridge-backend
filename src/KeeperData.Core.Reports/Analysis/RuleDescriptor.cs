using KeeperData.Core.Reports.Rules;

namespace KeeperData.Core.Reports.Analysis;

/// <summary>
/// Describes a rule along with its post-execution behavior configuration.
/// </summary>
/// <typeparam name="TInput">The type of input the rule operates on.</typeparam>
public sealed class RuleDescriptor<TInput>
{
    /// <summary>
    /// Gets the rule to execute.
    /// </summary>
    public required ICleanseRule<TInput> Rule { get; init; }

    /// <summary>
    /// Gets the predicate that determines if pipeline processing should stop based on the rule result.
    /// </summary>
    public required Func<RuleResult, bool> ShouldStopProcessing { get; init; }
}
