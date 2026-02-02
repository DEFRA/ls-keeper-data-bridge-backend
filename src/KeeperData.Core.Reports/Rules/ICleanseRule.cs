using KeeperData.Core.Reports.Analysis;

namespace KeeperData.Core.Reports.Rules;

/// <summary>
/// Represents a cleanse rule that analyzes input data and returns a result.
/// </summary>
/// <typeparam name="TInput">The type of input data the rule operates on.</typeparam>
public interface ICleanseRule<TInput>
{
    /// <summary>
    /// Gets the unique code identifying this rule.
    /// </summary>
    string RuleCode { get; }

    /// <summary>
    /// Executes the rule against the provided input.
    /// </summary>
    /// <param name="input">The input data to analyze.</param>
    /// <param name="context">The analysis context providing cached data access.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The result of the rule execution.</returns>
    Task<RuleResult> ExecuteAsync(TInput input, IAnalysisContext context, CancellationToken ct);
}
