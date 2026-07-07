using KeeperData.Core.Pipeline;

namespace KeeperData.Core.Pipeline;

/// <summary>MAP: one upstream item -> one downstream item. Framework owns the foreach; the
/// concrete stage streams internally inside <see cref="MapAsync"/> (open -> stream -> write -> swap).</summary>
public abstract class MapStage<TIn, TOut> : IStage<TIn, TOut>
{
    public abstract string Name { get; }

    public IAsyncEnumerable<TOut> RunAsync(IAsyncEnumerable<TIn> input, IPipelineContext context, CancellationToken cancellationToken)
        => throw new NotImplementedException("Framework: await foreach(input) -> skip-if-exists -> MapAsync -> yield.");

    protected abstract Task<TOut> MapAsync(TIn input, IPipelineContext context, CancellationToken cancellationToken);
}
