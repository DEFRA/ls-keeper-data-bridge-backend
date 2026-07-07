namespace KeeperData.Core.Pipeline;

/// <summary>One erased step in an assembled pipeline (name + boxed stage).</summary>
public sealed record PipelineStep(string Name, object Stage);
