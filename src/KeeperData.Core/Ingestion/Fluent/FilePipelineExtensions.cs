using KeeperData.Core.Pipeline;
using KeeperData.Core.Ingestion.Contracts;
using KeeperData.Core.Ingestion.Payloads;
using KeeperData.Core.Ingestion.Stages;

namespace KeeperData.Core.Ingestion.Fluent;

// One extension per stage; each is just builder.Then(new XStage(...)). The receiver and return
// types thread the stage order, so an out-of-order call will not compile.
public static class FilePipelineExtensions
{
    public static PipelineBuilder<DatasetFileSet> Discover(this PipelineBuilder<DiscoveredFile> builder, IDataSetDefinitions definitions)
        => builder.Then(new DiscoverStage(definitions));

    public static PipelineBuilder<RawFileSet> Decrypt(this PipelineBuilder<DatasetFileSet> builder, IPasswordPolicy passwordPolicy, IFileDecryptor decryptor)
        => builder.Then(new DecryptStage(passwordPolicy, decryptor));

    public static PipelineBuilder<NormalisedFileSet> Normalise(this PipelineBuilder<RawFileSet> builder, INormaliserFactory normalisers)
        => builder.Then(new NormaliseStage(normalisers));

    public static PipelineBuilder<SnapshotFile> Snapshot(this PipelineBuilder<NormalisedFileSet> builder)
        => builder.Then(new SnapshotStage());

    public static PipelineBuilder<StagingDatabase> LoadDuckDb(this PipelineBuilder<SnapshotFile> builder, IDuckDbStagingWriter stagingWriter)
        => builder.Then(new LoadDuckDbStage(stagingWriter));
}
