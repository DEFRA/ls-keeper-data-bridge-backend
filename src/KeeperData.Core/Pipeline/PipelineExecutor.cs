namespace KeeperData.Core.Pipeline;

public sealed class PipelineExecutor : IPipelineExecutor
{
    public Task RunAsync(PipelineDefinition pipelineDefinition, IPipelineContext context, CancellationToken cancellationToken)
        => throw new NotImplementedException("Framework: for each step -> log start -> skip if output exists -> run -> time -> log end.");
}
