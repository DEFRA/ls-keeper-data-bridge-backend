using System.Runtime.CompilerServices;
using KeeperData.Core.Pipeline;

namespace KeeperData.Core.EtlPipeline.Concurrency;

/// <summary>One item in, one out, with the items mapped concurrently rather than one after another.
///
/// Items are mapped against no budget on purpose. The work is inside each stage's own per-file loop,
/// and that is what holds the permits; a permit taken here and held while waiting for one there
/// would let the outer loop consume the budget and then wait on itself.
///
/// Output keeps the input's order, so a payload never depends on which item happened to finish
/// first.</summary>
public abstract class ParallelMapStage<TIn, TOut> : IStage<TIn, TOut>
{
    protected ParallelMapStage(EtlConcurrency concurrency) => Concurrency = concurrency;

    public abstract string Name { get; }

    protected EtlConcurrency Concurrency { get; }

    public async IAsyncEnumerable<TOut> RunAsync(
        IAsyncEnumerable<TIn> input,
        IPipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var items = new List<TIn>();

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            items.Add(item);
        }

        var mapped = await Concurrency.ForEachAsync(
            items,
            EtlWorkload.Unbounded,
            (item, token) => MapAsync(item, context, token),
            cancellationToken);

        foreach (var item in mapped)
        {
            yield return item;
        }
    }

    protected abstract Task<TOut> MapAsync(TIn input, IPipelineContext context, CancellationToken cancellationToken);
}
