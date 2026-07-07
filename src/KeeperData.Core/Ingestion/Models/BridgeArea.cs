namespace KeeperData.Core.Ingestion.Models;

/// <summary>The durable, checkpointed areas a run writes through, in order.</summary>
public enum BridgeArea { Raw, Normalised, Snapshots, Staging, Views }
