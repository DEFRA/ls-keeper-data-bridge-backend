namespace KeeperData.Core.Pipeline;

/// <summary>Walks the stages and owns the CROSS-CUTTING concerns - skip-if-output-exists,
/// timing, logging, error wrapping - so stages only stream-transform.</summary>
public interface IPipelineExecutor
{
    Task RunAsync(PipelineDefinition pipelineDefinition, IPipelineContext context, CancellationToken cancellationToken);
}
