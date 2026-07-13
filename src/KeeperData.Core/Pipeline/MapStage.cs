using System.Runtime.CompilerServices;

namespace KeeperData.Core.Pipeline;

/// <summary>One item in, one out. Plain mapping stage.</summary>
public abstract class MapStage<TIn, TOut> : IStage<TIn, TOut>
{
    public abstract string Name { get; }

    public async IAsyncEnumerable<TOut> RunAsync(IAsyncEnumerable<TIn> input, IPipelineContext context, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            yield return await MapAsync(item, context, cancellationToken);
        }
    }

    protected abstract Task<TOut> MapAsync(TIn input, IPipelineContext context, CancellationToken cancellationToken);
}
