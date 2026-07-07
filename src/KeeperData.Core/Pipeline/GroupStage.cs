using KeeperData.Core.Pipeline;

namespace KeeperData.Core.Pipeline;

/// <summary>GROUP: many upstream items -> fewer downstream items (e.g. group discovered files
/// into per-dataset sets).</summary>
public abstract class GroupStage<TIn, TOut> : IStage<TIn, TOut>
{
    public abstract string Name { get; }

    public IAsyncEnumerable<TOut> RunAsync(IAsyncEnumerable<TIn> input, IPipelineContext context, CancellationToken cancellationToken)
        => throw new NotImplementedException("Framework: buffer/partition input, then delegate to GroupAsync.");

    protected abstract IAsyncEnumerable<TOut> GroupAsync(IAsyncEnumerable<TIn> input, IPipelineContext context, CancellationToken cancellationToken);
}
