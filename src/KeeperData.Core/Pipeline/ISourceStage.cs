namespace KeeperData.Core.Pipeline;

public interface ISourceStage<out TOut>
{
    string Name { get; }
    IAsyncEnumerable<TOut> RunAsync(IPipelineContext context, CancellationToken cancellationToken);
}
