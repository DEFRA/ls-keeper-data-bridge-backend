using System.Runtime.CompilerServices;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.Pipeline;

namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>First stage. Asks the external catalogue which files exist for each dataset and emits one
/// DiscoveredFileSet per dataset that has files. Does not open or read any file.</summary>
public sealed class DiscoverFilesStage(IExternalCatalogueService catalogue) : ISourceStage<DiscoveredFileSet>
{
    public string Name => "discover-files";

    public async IAsyncEnumerable<DiscoveredFileSet> RunAsync(
        IPipelineContext context, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var lookbackDays = context is EtlPipelineContext etl ? etl.LookbackDays : 0;

        var fileSets = await catalogue.GetFileSetsAsync(lookbackDays, cancellationToken);

        foreach (var fileSet in fileSets)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (fileSet.Files.Length == 0)
            {
                continue;
            }

            yield return new DiscoveredFileSet(fileSet.Definition, fileSet.Files);
        }
    }
}
