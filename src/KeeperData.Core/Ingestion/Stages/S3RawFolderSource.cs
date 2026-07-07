using KeeperData.Core.Pipeline;
using KeeperData.Core.Ingestion.Contracts;
using KeeperData.Core.Ingestion.Payloads;
#pragma warning disable CS9113 // Parameter is unread.

namespace KeeperData.Core.Ingestion.Stages;

/// <summary>HEAD of the pipeline. Lists the external bucket, matches each object to a dataset
/// definition, parses its timestamp and main/delta flag. Yields one <see cref="DiscoveredFile"/> per object.</summary>
public sealed class S3RawFolderSource(IDataSetDefinitions definitions) : IFileSource
{
    public string Name => "source:external";

    public IAsyncEnumerable<DiscoveredFile> RunAsync(IPipelineContext context, CancellationToken cancellationToken)
        // STREAMS: list external keys -> match prefix per definition -> parse timestamp -> yield DiscoveredFile
        => throw new NotImplementedException();
}
