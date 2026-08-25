namespace KeeperData.Core.Pipeline;

/// <summary>Many in, fewer out. Partition/reduce the stream.</summary>
public abstract class GroupStage<TIn, TOut> : IStage<TIn, TOut>
{
    public abstract string Name { get; }

    public IAsyncEnumerable<TOut> RunAsync(IAsyncEnumerable<TIn> input, IPipelineContext context, CancellationToken cancellationToken)
        => GroupAsync(input, context, cancellationToken);

    protected abstract IAsyncEnumerable<TOut> GroupAsync(IAsyncEnumerable<TIn> input, IPipelineContext context, CancellationToken cancellationToken);
}
