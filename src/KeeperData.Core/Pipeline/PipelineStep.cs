namespace KeeperData.Core.Pipeline;

// One erased step: its name and a function that runs the (type-erased) stream through the stage.
// StageAdapter lets the executor walk a heterogeneously-typed chain as a single list.
internal sealed record PipelineStep(
    string Name,
    Func<IAsyncEnumerable<object>, IPipelineContext, CancellationToken, IAsyncEnumerable<object>> Invoke);
