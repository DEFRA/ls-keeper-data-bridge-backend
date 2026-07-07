using KeeperData.Core.Ingestion.Models;

namespace KeeperData.Core.Ingestion.Contracts;

/// <summary>Facade over the durable areas (raw/ normalised/ snapshots/ staging/).</summary>
public interface IDataBridgeStore
{
    IBlobArea Area(BridgeArea area);
}
