using KeeperData.Core.Pipeline;
using KeeperData.Core.Ingestion.Contracts;
using KeeperData.Core.Ingestion.Payloads;
#pragma warning disable CS9113 // Parameter is unread.

namespace KeeperData.Core.Ingestion.Stages;

/// <summary>Decrypts a dataset's main + deltas into raw/ using the
/// password policy. Materialises: raw/. Idempotent: skip files already present.</summary>
public sealed class DecryptStage(IPasswordPolicy passwordPolicy, IFileDecryptor decryptor) : MapStage<DatasetFileSet, RawFileSet>
{
    public override string Name => "decrypt";

    protected override Task<RawFileSet> MapAsync(DatasetFileSet input, IPipelineContext context, CancellationToken cancellationToken)
        // STREAMS per file: open source -> DerivePassword -> IFileDecryptor.DecryptAsync -> WriteAtomic to raw/
        => throw new NotImplementedException();
}
