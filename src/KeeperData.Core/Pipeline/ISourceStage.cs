using KeeperData.Core.Pipeline;

namespace KeeperData.Core.Pipeline;

/// <summary>A stage with no upstream - the head of the pipeline.</summary>
public interface ISourceStage<TOut>
{
    string Name { get; }
    IAsyncEnumerable<TOut> RunAsync(IPipelineContext context, CancellationToken cancellationToken);
}
