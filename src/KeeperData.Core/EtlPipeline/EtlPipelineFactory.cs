using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.Pipeline;

namespace KeeperData.Core.EtlPipeline;

/// <summary>Defines the ETL pipeline. The single place the stage order lives.
/// Later stages (decrypt, normalise, snapshot, load) are appended here as they land.</summary>
public sealed class EtlPipelineFactory(IExternalCatalogueService catalogue) : IEtlPipelineFactory
{
    public PipelineDefinition Create()
        => PipelineBuilder
            .InputSource(new DiscoverFilesStage(catalogue))
            .Build();
}
