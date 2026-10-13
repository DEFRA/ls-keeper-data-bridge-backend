using KeeperData.Core.Pipeline;

namespace KeeperData.Core.EtlPipeline;

/// <summary>Run context for the ETL pipeline. Stages take their dependencies via their constructors;
/// this carries the per-run values passed through by the coordinator.
///
/// There is deliberately no lookback: discovery lists everything the datasets match. A window made
/// the listing no cheaper and could exclude a dataset's baseline, which fails silently.</summary>
public sealed class EtlPipelineContext(Guid runId, string sourceType, string? dataset = null) : IPipelineContext
{
    public Guid RunId { get; } = runId;

    /// <summary>Restricts the run to a single dataset by name. Null runs every configured dataset.</summary>
    public string? Dataset { get; } = dataset;

    /// <summary>The blob storage source for this run.</summary>
    public string SourceType { get; } = sourceType;
}
