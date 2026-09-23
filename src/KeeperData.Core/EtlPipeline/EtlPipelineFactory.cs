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
    NormaliseStage normaliseStage,
    OptimiseStage optimiseStage,
    SnapshotStage snapshotStage,
    LoadDuckDbStage loadDuckDbStage,
    ExportSqliteStage exportSqliteStage) : IEtlPipelineFactory
{
    public PipelineDefinition Create()
        => PipelineBuilder
            .InputSource(new S3RawFolderSource(catalogueFactory))
            .Discover()               // -> DiscoveredFileSet
            .Decrypt(decryptStage)    // -> RawFileSet        (raw/)
            .Normalise(normaliseStage) // -> NormalisedFileSet (normalised/*.parquet)
            .Optimise(optimiseStage)  // -> OptimisedFileSet  (optimised/*.parquet)
            .Snapshot(snapshotStage)  // -> SnapshotFile      (snapshots/*.parquet)
            .LoadDuckDb(loadDuckDbStage) // -> StagingDatabase (staging/*.duckdb)
            .ExportSqlite(exportSqliteStage) // -> SqliteExportFile (views/*.sqlite)
            .Build();

    // TEMP: expanded for step-through debugging. Revert to the fluent chain afterwards.
//    var source = new S3RawFolderSource(catalogueFactory);
//    var builder0 = PipelineBuilder.InputSource(source);

//    var builder1 = builder0.Then(new DiscoverStage());   // -> DiscoveredFileSet
//    var builder2 = builder1.Then(decryptStage);           // -> RawFileSet        (raw/)
//    var builder3 = builder2.Then(normaliseStage);         // -> NormalisedFileSet (normalised/*.parquet)
//    var builder4 = builder3.Then(optimiseStage);          // -> OptimisedFileSet  (optimised/*.parquet)
//    var builder5 = builder4.Then(snapshotStage);          // -> SnapshotFile      (snapshots/*.parquet)
//    var builder6 = builder5.Then(loadDuckDbStage);        // -> StagingDatabase   (staging/*.duckdb)
//    var builder7 = builder6.Then(exportSqliteStage);      // -> SqliteExportFile  (views/*.sqlite)

//    var pipeline = builder7.Build();

//        return pipeline;
//}
