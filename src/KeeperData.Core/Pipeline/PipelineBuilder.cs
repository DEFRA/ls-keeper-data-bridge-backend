using KeeperData.Core.Ingestion.Contracts;
using KeeperData.Core.Ingestion.Payloads;

namespace KeeperData.Core.Pipeline;

// Two levels of granularity: the fluent chain stitches durable, checkpointed stages (each writes a
// folder the next reads); inside a stage the work streams (IAsyncEnumerable), never fully in memory.
/// <summary>Fluent entry point: <c>PipelineBuilder.InputSource(src).Discover()...Build()</c>.</summary>
public static class PipelineBuilder
{
    public static PipelineBuilder<DiscoveredFile> InputSource(IFileSource source)
        => PipelineBuilder<DiscoveredFile>.FromSource(source);
}
