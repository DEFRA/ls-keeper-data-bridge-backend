using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.EtlPipeline.Fluent;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.Pipeline;

namespace KeeperData.Core.EtlPipeline;

/// <summary>Defines the ETL pipeline. The single place the stage order lives.
/// Implementing a stage does not require changing this file (only adding a dependency does).</summary>
public sealed class EtlPipelineFactory(
    IExternalCatalogueServiceFactory catalogueFactory,
    DecryptStage decryptStage,
    SnapshotStage snapshotStage) : IEtlPipelineFactory
{
    public PipelineDefinition Create()
        => PipelineBuilder
            .InputSource(new S3RawFolderSource(catalogueFactory))
            .Discover()               // -> DiscoveredFileSet
            .Decrypt(decryptStage)    // -> RawFileSet        (raw/)
            .Normalise()              // -> NormalisedFileSet (normalised/*.parquet)
            .Snapshot(snapshotStage)  // -> SnapshotFile      (snapshots/*.parquet)
            .LoadDuckDb()             // -> StagingDatabase   (staging/*.duckdb)
            .Build();
}
