namespace KeeperData.Core.Pipeline;

/// <summary>An assembled, immutable pipeline: an ordered list of stages ready for the executor.</summary>
public sealed class PipelineDefinition
{
    public IReadOnlyList<PipelineStep> Steps { get; }
    internal PipelineDefinition(IReadOnlyList<PipelineStep> steps) => Steps = steps;
}
