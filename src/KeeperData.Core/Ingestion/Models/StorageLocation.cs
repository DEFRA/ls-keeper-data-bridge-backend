namespace KeeperData.Core.Ingestion.Models;

/// <summary>A location inside one of the durable areas, e.g. (Normalised, "AMESHAULIER.parquet").</summary>
public sealed record StorageLocation(BridgeArea Area, string Key);
