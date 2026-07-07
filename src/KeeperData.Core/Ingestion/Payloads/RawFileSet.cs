using KeeperData.Core.Ingestion.Models;

namespace KeeperData.Core.Ingestion.Payloads;

/// <summary>The same set, decrypted into raw/.</summary>
public sealed record RawFileSet(
    DataSetDefinition Dataset,
    StorageLocation Main,
    IReadOnlyList<StorageLocation> Deltas);
