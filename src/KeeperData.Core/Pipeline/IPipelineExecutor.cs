namespace KeeperData.Core.Pipeline;

/// <summary>Runs a PipelineDefinition: walks the stages in order, feeding each stage's
/// output into the next, and owns the cross-cutting concerns (timing, logging).</summary>
public interface IPipelineExecutor
{
    Task RunAsync(PipelineDefinition pipeline, IPipelineContext context, CancellationToken cancellationToken);
}
