namespace KeeperData.Core.Pipeline;

public interface ISourceStage<TOut>
{
    string Name { get; }
    IAsyncEnumerable<TOut> RunAsync(IPipelineContext context, CancellationToken cancellationToken);
}
