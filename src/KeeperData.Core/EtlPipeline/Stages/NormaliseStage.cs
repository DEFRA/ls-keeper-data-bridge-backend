using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.Pipeline;

namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>Converts each raw file (PSV / legacy H-C-D-T) to Parquet in normalised/. No DuckDB here.
/// STUB - passes through. The owner adds the normaliser dependency and implements MapAsync.</summary>
public sealed class NormaliseStage : MapStage<RawFileSet, NormalisedFileSet>
{
    public override string Name => "normalise";

    protected override Task<NormalisedFileSet> MapAsync(RawFileSet input, IPipelineContext context, CancellationToken cancellationToken)
        => Task.FromResult(new NormalisedFileSet(input.Definition));
}
