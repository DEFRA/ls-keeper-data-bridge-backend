using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.EtlPipeline.Payloads;

/// <summary>A dataset's files decrypted into raw/. Output of decrypt, input to normalise.
///
/// Carries keys only, never content. The decrypted bytes live in raw/ in the internal bucket;
/// normalise streams them back out by key.</summary>
public sealed record RawFileSet(DataSetDefinition Definition)
{
    public Guid RunId { get; init; }

    /// <summary>The object keys written to (or already present in) raw/, relative to the raw folder.</summary>
    public IReadOnlyList<string> Files { get; init; } = [];
}
