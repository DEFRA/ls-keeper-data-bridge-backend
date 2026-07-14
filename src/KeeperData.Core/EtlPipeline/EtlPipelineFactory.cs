using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.EtlPipeline.Fluent;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.EtlPipeline;

/// <summary>Defines the ETL pipeline. The single place the stage order lives.
/// Later stages (decrypt, normalise, snapshot, load) are appended here as they land.</summary>
public sealed class EtlPipelineFactory(
    IExternalCatalogueServiceFactory catalogueFactory,
    IBlobStorageServiceFactory blobStorageFactory,
    ILogger<ReportDiscoveredFilesStage> reportLogger) : IEtlPipelineFactory
{
    public PipelineDefinition Create()
        => PipelineBuilder
            .InputSource(new DiscoverFilesStage(catalogueFactory))
            .ReportDiscoveredFiles(blobStorageFactory, reportLogger)   // TEMP ONLY (for "manual testing purposes") - remove during next stage implementation
            .Build();
}
