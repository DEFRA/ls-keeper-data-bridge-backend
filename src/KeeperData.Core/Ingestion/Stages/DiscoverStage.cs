using KeeperData.Core.Pipeline;
using KeeperData.Core.Ingestion.Contracts;
using KeeperData.Core.Ingestion.Payloads;
#pragma warning disable CS9113 // Parameter is unread.

namespace KeeperData.Core.Ingestion.Stages;

/// <summary>Groups discovered files by dataset, orders deltas
/// oldest->newest, picks the main/seed. Identifies deltas; does NOT open them.</summary>
public sealed class DiscoverStage(IDataSetDefinitions definitions) 
    : GroupStage<DiscoveredFile, DatasetFileSet>
{
    public override string Name => "discover";

    protected override IAsyncEnumerable<DatasetFileSet> GroupAsync(IAsyncEnumerable<DiscoveredFile> input, IPipelineContext context, CancellationToken cancellationToken)
        // STREAMS: partition input by dataset -> order deltas by timestamp -> emit one DatasetFileSet each
        => throw new NotImplementedException();
}
