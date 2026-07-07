using KeeperData.Core.Pipeline;

namespace KeeperData.Core.Pipeline;

/// <summary>AGGREGATE: the entire upstream -> a single output (e.g. load all snapshots into one DuckDB).</summary>
public abstract class AggregateStage<TIn, TOut> : IStage<TIn, TOut>
{
    public abstract string Name { get; }

    public IAsyncEnumerable<TOut> RunAsync(IAsyncEnumerable<TIn> input, IPipelineContext context, CancellationToken cancellationToken)
        => throw new NotImplementedException("Framework: drain input to a list, call AggregateAsync, yield the one result.");

    protected abstract Task<TOut> AggregateAsync(IReadOnlyList<TIn> all, IPipelineContext context, CancellationToken cancellationToken);
}
