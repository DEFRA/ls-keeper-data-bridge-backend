using KeeperData.Core.Pipeline;
using KeeperData.Core.Ingestion.Contracts;
using KeeperData.Core.Ingestion.Stages;
using KeeperData.Core.Ingestion.Fluent;

namespace KeeperData.Core.Ingestion;

/// <summary>Composes the file pipeline into a <see cref="PipelineDefinition"/> ready for the
/// executor. The single place the stage order lives.</summary>
public static class FilePipelineFactory
{
    public static PipelineDefinition Create(
        IDataSetDefinitions definitions,
        IPasswordPolicy passwordPolicy,
        IFileDecryptor decryptor,
        INormaliserFactory normalisers,
        IDuckDbStagingWriter stagingWriter)
        => PipelineBuilder
            .InputSource(new S3RawFolderSource(definitions))
            .Discover(definitions)               // -> DatasetFileSet    (main + deltas)
            .Decrypt(passwordPolicy, decryptor)  // -> RawFileSet        (raw/)
            .Normalise(normalisers)              // -> NormalisedFileSet (normalised/*.parquet)
            .Snapshot()                          // -> SnapshotFile      (snapshots/*.parquet)
            .LoadDuckDb(stagingWriter)           // -> StagingDatabase   (staging/*.duckdb)
            .Build();
}
