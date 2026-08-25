using System.Runtime.CompilerServices;

namespace KeeperData.Core.Pipeline;

// One step in the pipeline: its name plus a function that runs the object-typed stream through
// the stage. StageAdapter handles the typed-to-object conversion, so the executor can walk
// stages with different input/output types as one uniform list.
internal static class StageAdapter
{
    public static async IAsyncEnumerable<object> FromSource<T>(
        ISourceStage<T> source, IPipelineContext context, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var item in source.RunAsync(context, cancellationToken).WithCancellation(cancellationToken))
        {
            yield return item!;
        }
    }

    public static async IAsyncEnumerable<object> FromStage<TIn, TOut>(
        IStage<TIn, TOut> stage, IAsyncEnumerable<object> input, IPipelineContext context, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var item in stage.RunAsync(Downcast<TIn>(input, cancellationToken), context, cancellationToken).WithCancellation(cancellationToken))
        {
            yield return item!;
        }
    }

    private static async IAsyncEnumerable<TIn> Downcast<TIn>(
        IAsyncEnumerable<object> input, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            yield return (TIn)item;
        }
    }
}
