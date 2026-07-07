using KeeperData.Core.Ingestion.Models;

namespace KeeperData.Core.Ingestion.Payloads;

/// <summary>The same set, converted to Parquet in normalised/.</summary>
public sealed record NormalisedFileSet(
    DataSetDefinition Dataset,
    StorageLocation Main,
    IReadOnlyList<StorageLocation> Deltas);
