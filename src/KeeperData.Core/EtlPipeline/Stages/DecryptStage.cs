using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.Pipeline;

namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>Decrypts a dataset's files into raw/. Materialises: raw/. Idempotent: skip files already present.
/// STUB - passes through. The owner adds the decryptor/password-policy dependency and implements MapAsync.</summary>
public sealed class DecryptStage : MapStage<DiscoveredFileSet, RawFileSet>
{
    public override string Name => "decrypt";

    protected override Task<RawFileSet> MapAsync(DiscoveredFileSet input, IPipelineContext context, CancellationToken cancellationToken)
        => Task.FromResult(new RawFileSet(input.Definition));
}
