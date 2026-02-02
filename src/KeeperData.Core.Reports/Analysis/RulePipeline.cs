namespace KeeperData.Core.Reports.Analysis;

/// <summary>
/// Executes a sequence of rules, respecting stop conditions after each rule.
/// </summary>
/// <typeparam name="TInput">The type of input data.</typeparam>
public sealed class RulePipeline<TInput> : IRulePipeline<TInput>
{
    private readonly IReadOnlyList<RuleDescriptor<TInput>> _rules;

    public RulePipeline(IReadOnlyList<RuleDescriptor<TInput>> rules)
    {
        _rules = rules;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<PipelineRuleResult>> ExecuteAsync(
        TInput input,
        IAnalysisContext context,
        CancellationToken ct)
    {
        var results = new List<PipelineRuleResult>();

        foreach (var descriptor in _rules)
        {
            var result = await descriptor.Rule.ExecuteAsync(input, context, ct);
            var shouldStop = descriptor.ShouldStopProcessing(result);

            results.Add(new PipelineRuleResult
            {
                Result = result,
                RuleCode = descriptor.Rule.RuleCode,
                StopProcessing = shouldStop
            });

            if (shouldStop)
            {
                break;
            }
        }

        return results;
    }
}
