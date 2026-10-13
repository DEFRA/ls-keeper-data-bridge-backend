using KeeperData.Core.EtlPipeline.Stages;

namespace KeeperData.Core.EtlPipeline;

/// <summary>The pipeline's stages, resolved as one dependency so adding a stage does not widen
/// <see cref="EtlPipelineFactory"/>'s constructor.</summary>
public sealed record EtlStages(
    DecryptStage Decrypt,
    NormaliseStage Normalise,
    OptimiseStage Optimise,
    SnapshotStage Snapshot,
    LoadDuckDbStage LoadDuckDb,
    ExportSqliteStage ExportSqlite);
