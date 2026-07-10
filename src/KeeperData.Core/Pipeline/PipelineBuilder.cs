namespace KeeperData.Core.Pipeline;

/// <summary>Fluent entry point: <c>PipelineBuilder.InputSource(src).Then(...).Build()</c>.</summary>
public static class PipelineBuilder
{
    public static PipelineBuilder<TOut> InputSource<TOut>(ISourceStage<TOut> source)
        => new((context, cancellationToken) => StageAdapter.FromSource(source, context, cancellationToken), []);
}

/// <summary>Immutable, typed builder. <see cref="Then{TNext}"/> threads the output type to the next
/// stage's input at compile time, so an out-of-order stage will not compile.</summary>
public sealed class PipelineBuilder<TOut>
{
    private readonly Func<IPipelineContext, CancellationToken, IAsyncEnumerable<object>> _source;
    private readonly PipelineStep[] _steps;

    internal PipelineBuilder(
        Func<IPipelineContext, CancellationToken, IAsyncEnumerable<object>> source,
        PipelineStep[] steps)
    {
        _source = source;
        _steps = steps;
    }

    public PipelineBuilder<TNext> Then<TNext>(IStage<TOut, TNext> stage)
    {
        var step = new PipelineStep(
            stage.Name,
            (input, context, cancellationToken) => StageAdapter.FromStage(stage, input, context, cancellationToken));
        return new PipelineBuilder<TNext>(_source, [.. _steps, step]);
    }

    public PipelineDefinition Build() => new(_source, _steps);
}
