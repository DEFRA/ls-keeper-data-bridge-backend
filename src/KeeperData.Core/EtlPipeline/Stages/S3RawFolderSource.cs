using System.Runtime.CompilerServices;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.Pipeline;

namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>Head of the pipeline. Lists the source for the run and yields one DiscoveredFile per
/// object, matched to its dataset. Does not group and does not open any file.</summary>
public sealed class S3RawFolderSource(IExternalCatalogueServiceFactory catalogueFactory) : ISourceStage<DiscoveredFile>
{
    public string Name => "source:external";

    public async IAsyncEnumerable<DiscoveredFile> RunAsync(
        IPipelineContext context, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var etlContext = (EtlPipelineContext)context;

        var catalogue = catalogueFactory.Create(etlContext.SourceType);
        var fileSets = await catalogue.GetFileSetsAsync(etlContext.LookbackDays, cancellationToken);

        foreach (var fileSet in fileSets)
        {
            foreach (var file in fileSet.Files)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return new DiscoveredFile(fileSet.Definition, file);
            }
        }
    }
}
