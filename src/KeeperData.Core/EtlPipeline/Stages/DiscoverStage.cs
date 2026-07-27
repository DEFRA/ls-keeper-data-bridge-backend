using System.Runtime.CompilerServices;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.Pipeline;

namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>Groups the discovered files by dataset. Datasets with no files are dropped.
/// Identifies which files belong together; does not open them.</summary>
public sealed class DiscoverStage : GroupStage<DiscoveredFile, DiscoveredFileSet>
{
    public override string Name => "discover";

    protected override async IAsyncEnumerable<DiscoveredFileSet> GroupAsync(
        IAsyncEnumerable<DiscoveredFile> input,
        IPipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var byDataset = new Dictionary<string, (DataSetDefinition Definition, List<EtlFile> Files)>();

        await foreach (var discovered in input.WithCancellation(cancellationToken))
        {
            if (!byDataset.TryGetValue(discovered.Definition.Name, out var entry))
            {
                entry = (discovered.Definition, []);
                byDataset[discovered.Definition.Name] = entry;
            }

            entry.Files.Add(discovered.File);
        }

        foreach (var (definition, files) in byDataset.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (files.Count == 0)
            {
                continue;
            }

            yield return new DiscoveredFileSet(definition, files);
        }
    }
}
