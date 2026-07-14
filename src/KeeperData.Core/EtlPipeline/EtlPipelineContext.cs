using KeeperData.Core.Pipeline;

namespace KeeperData.Core.EtlPipeline;

/// <summary>Run context for the ETL pipeline. Stages take their dependencies via their constructors;
/// this carries per-run values only.</summary>
public sealed class EtlPipelineContext(Guid runId, int lookbackDays) : IPipelineContext
{
    public Guid RunId { get; } = runId;

    /// <summary>Days to look back for files. 0 means today only.</summary>
    public int LookbackDays { get; } = lookbackDays;
}
