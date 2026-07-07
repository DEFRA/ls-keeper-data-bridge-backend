using KeeperData.Core.Pipeline;

namespace KeeperData.Core.Pipeline;

/// <summary>Immutable, typed builder. Each <see cref="Then{TNext}"/> threads the output type
/// to the next stage's input type at COMPILE time - a wrong stage order won't compile.</summary>
public sealed class PipelineBuilder<TOut>
{
    private readonly PipelineStep[] _steps;

    internal PipelineBuilder(PipelineStep[] steps) => _steps = steps;

    internal static PipelineBuilder<TSource> FromSource<TSource>(ISourceStage<TSource> source)
        => new([new PipelineStep(source.Name, source)]);

    public PipelineBuilder<TNext> Then<TNext>(IStage<TOut, TNext> stage)
        => new([.. _steps, new PipelineStep(stage.Name, stage)]);

    public PipelineDefinition Build() => new(_steps);
}
