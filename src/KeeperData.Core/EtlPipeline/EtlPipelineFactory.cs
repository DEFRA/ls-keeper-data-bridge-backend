using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.EtlPipeline.Fluent;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.Pipeline;

namespace KeeperData.Core.EtlPipeline;

/// <summary>Defines the ETL pipeline. The single place the stage order lives.
/// Implementing a stage does not require changing this file (only adding a dependency does).</summary>
public sealed class EtlPipelineFactory(
    IExternalCatalogueServiceFactory catalogueFactory,
    IDataSetDefinitions dataSetDefinitions,
    EtlStages stages) : IEtlPipelineFactory
{
    public PipelineDefinition Create()
        => PipelineBuilder
            .InputSource(new S3RawFolderSource(catalogueFactory, dataSetDefinitions))
            .Discover()               // -> DiscoveredFileSet
            .Decrypt(stages.Decrypt)    // -> RawFileSet        (raw/)
            .Normalise(stages.Normalise) // -> NormalisedFileSet (normalised/*.parquet)
            .Optimise(stages.Optimise)  // -> OptimisedFileSet  (optimised/*.parquet)
            .Snapshot(stages.Snapshot)  // -> SnapshotFile      (snapshots/*.parquet)
            .LoadDuckDb(stages.LoadDuckDb) // -> StagingDatabase (staging/*.duckdb)
            .ExportSqlite(stages.ExportSqlite) // -> SqliteExportFile (views/*.sqlite)
            .Build();
}
