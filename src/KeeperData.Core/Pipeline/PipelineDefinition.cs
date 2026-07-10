namespace KeeperData.Core.Pipeline;

/// <summary>An assembled, runnable pipeline: a source plus an ordered list of stages.</summary>
public sealed class PipelineDefinition
{
    internal Func<IPipelineContext, CancellationToken, IAsyncEnumerable<object>> Source { get; }
    internal IReadOnlyList<PipelineStep> Steps { get; }

    internal PipelineDefinition(
        Func<IPipelineContext, CancellationToken, IAsyncEnumerable<object>> source,
        IReadOnlyList<PipelineStep> steps)
    {
        Source = source;
        Steps = steps;
    }

    /// <summary>Stage names in order (useful for inspection and tests).</summary>
    public IReadOnlyList<string> StageNames => Steps.Select(s => s.Name).ToList();
}
