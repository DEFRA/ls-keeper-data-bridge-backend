using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.EtlPipeline.Fluent;

/// <summary>Fluent stage extensions for the ETL pipeline. One method per stage.</summary>
public static class PipelineExtensions
{
    // This specific extension method, is temporarily. It's only to test the discovery stage of the pipeline, 
    // since we do not have a complete pipeline yet, so it outputs the discovered files, that's testable by a tester.
    // Remove this method and ReportDiscoveredFilesStage once the pipeline runs through all the stages.
    public static PipelineBuilder<DiscoveredFileSet> ReportDiscoveredFiles(
        this PipelineBuilder<DiscoveredFileSet> builder,
        IBlobStorageServiceFactory blobStorageFactory,
        ILogger<ReportDiscoveredFilesStage> logger)
        => builder.Then(new ReportDiscoveredFilesStage(blobStorageFactory, logger));
}
