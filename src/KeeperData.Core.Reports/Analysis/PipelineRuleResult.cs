namespace KeeperData.Core.Reports.Analysis;

/// <summary>
/// Represents the result of a rule execution along with pipeline control information.
/// </summary>
public sealed record PipelineRuleResult
{
    /// <summary>
    /// Gets the underlying rule result.
    /// </summary>
    public required RuleResult Result { get; init; }

    /// <summary>
    /// Gets the rule code that produced this result.
    /// </summary>
    public required string RuleCode { get; init; }

    /// <summary>
    /// Gets whether the pipeline should stop processing further rules.
    /// </summary>
    public bool StopProcessing { get; init; }
}
