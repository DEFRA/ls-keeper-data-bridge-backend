using KeeperData.Core.Reports.Rules;

namespace KeeperData.Core.Reports.Analysis;

/// <summary>
/// Fluent builder for constructing rule pipelines with post-execution behavior.
/// </summary>
/// <typeparam name="TInput">The type of input data for the rules.</typeparam>
public sealed class RulePipelineBuilder<TInput>
{
    private readonly List<RuleDescriptor<TInput>> _rules = [];

    private RulePipelineBuilder() { }

    /// <summary>
    /// Creates a new pipeline builder.
    /// </summary>
    public static RulePipelineBuilder<TInput> Create() => new();

    /// <summary>
    /// Adds a rule to the pipeline and returns a configuration object for specifying behavior.
    /// </summary>
    /// <param name="rule">The rule to add.</param>
    /// <returns>A configuration object for specifying post-execution behavior.</returns>
    public RuleConfiguration AddRule(ICleanseRule<TInput> rule) => new(this, rule);

    /// <summary>
    /// Builds the configured pipeline.
    /// </summary>
    /// <returns>The constructed rule pipeline.</returns>
    public IRulePipeline<TInput> Build() => new RulePipeline<TInput>(_rules);

    /// <summary>
    /// Configuration for a rule's post-execution behavior.
    /// </summary>
    public sealed class RuleConfiguration
    {
        private readonly RulePipelineBuilder<TInput> _builder;
        private readonly ICleanseRule<TInput> _rule;

        internal RuleConfiguration(RulePipelineBuilder<TInput> builder, ICleanseRule<TInput> rule)
        {
            _builder = builder;
            _rule = rule;
        }

        /// <summary>
        /// Configures the pipeline to stop processing when the predicate returns true.
        /// </summary>
        /// <param name="predicate">Predicate that receives the rule result and returns true to stop processing.</param>
        /// <returns>The builder for method chaining.</returns>
        public RulePipelineBuilder<TInput> StopProcessingWhen(Func<RuleResult, bool> predicate)
        {
            _builder._rules.Add(new RuleDescriptor<TInput>
            {
                Rule = _rule,
                ShouldStopProcessing = predicate
            });
            return _builder;
        }

        /// <summary>
        /// Configures the pipeline to always continue to the next rule regardless of result.
        /// </summary>
        /// <returns>The builder for method chaining.</returns>
        public RulePipelineBuilder<TInput> ContinueAlways() => StopProcessingWhen(_ => false);

        /// <summary>
        /// Configures the pipeline to stop processing if the rule found an issue.
        /// </summary>
        /// <returns>The builder for method chaining.</returns>
        public RulePipelineBuilder<TInput> StopOnIssue() => StopProcessingWhen(r => r.HasIssue);
    }
}
