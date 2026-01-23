namespace KeeperData.Core.Reports.Analysis;

/// <summary>
/// Executes a pipeline of rules against input data.
/// </summary>
/// <typeparam name="TInput">The type of input data.</typeparam>
public interface IRulePipeline<TInput>
{
    /// <summary>
    /// Executes all rules in the pipeline until completion or a stop condition is met.
    /// </summary>
    /// <param name="input">The input data to analyze.</param>
    /// <param name="context">The analysis context.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The results from each executed rule.</returns>
    Task<IReadOnlyList<PipelineRuleResult>> ExecuteAsync(TInput input, IAnalysisContext context, CancellationToken ct);
}
