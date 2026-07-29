using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Pipeline;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.EtlPipeline.Fluent;

/// <summary>Fluent stage extensions for the ETL pipeline. One method per stage.
/// When a stage gains a dependency, add its parameter here and in the factory.</summary>
public static class PipelineExtensions
{
    public static PipelineBuilder<DiscoveredFileSet> Discover(this PipelineBuilder<DiscoveredFile> builder)
        => builder.Then(new DiscoverStage());

    public static PipelineBuilder<RawFileSet> Decrypt(this PipelineBuilder<DiscoveredFileSet> builder)
        => builder.Then(new DecryptStage());

    public static PipelineBuilder<NormalisedFileSet> Normalise(this PipelineBuilder<RawFileSet> builder)
        => builder.Then(new NormaliseStage());

    public static PipelineBuilder<SnapshotFile> Snapshot(
        this PipelineBuilder<NormalisedFileSet> builder,
        IEtlPipelineStorageProvider storageProvider,
        TimeProvider timeProvider,
        ILogger<SnapshotStage> logger)
        => builder.Then(new SnapshotStage(storageProvider, timeProvider, logger));

    public static PipelineBuilder<StagingDatabase> LoadDuckDb(this PipelineBuilder<SnapshotFile> builder)
        => builder.Then(new LoadDuckDbStage());
}
