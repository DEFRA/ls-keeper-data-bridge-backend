namespace KeeperData.Core.EtlPipeline.Models;

/// <summary>A location inside one of the durable areas, e.g. (Normalised, "sam_cph_holdings.parquet").</summary>
public sealed record StorageLocation(BridgeArea Area, string Key);
