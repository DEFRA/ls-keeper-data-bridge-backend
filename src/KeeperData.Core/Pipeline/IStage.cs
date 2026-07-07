using KeeperData.Core.Pipeline;

namespace KeeperData.Core.Pipeline;

/// <summary>A stage that stream-transforms upstream items into downstream items.</summary>
public interface IStage<TIn, TOut>
{
    string Name { get; }
    IAsyncEnumerable<TOut> RunAsync(IAsyncEnumerable<TIn> input, IPipelineContext context, CancellationToken cancellationToken);
}
