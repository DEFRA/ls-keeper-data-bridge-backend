namespace KeeperData.Core.Pipeline;

public interface IStage<TIn, TOut>
{
    string Name { get; }
    IAsyncEnumerable<TOut> RunAsync(IAsyncEnumerable<TIn> input, IPipelineContext context, CancellationToken cancellationToken);
}
