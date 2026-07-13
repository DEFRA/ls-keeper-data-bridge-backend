using System.Runtime.CompilerServices;

namespace KeeperData.Core.Pipeline;

/// <summary>One stream in, one out. Aggregation stage </summary>
public abstract class AggregateStage<TIn, TOut> : IStage<TIn, TOut>
{
    public abstract string Name { get; }

    public async IAsyncEnumerable<TOut> RunAsync(IAsyncEnumerable<TIn> input, IPipelineContext context, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var all = new List<TIn>();
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            all.Add(item);
        }
        yield return await AggregateAsync(all, context, cancellationToken);
    }

    protected abstract Task<TOut> AggregateAsync(IReadOnlyList<TIn> all, IPipelineContext context, CancellationToken cancellationToken);
}
