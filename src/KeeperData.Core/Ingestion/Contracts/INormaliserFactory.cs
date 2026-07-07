using KeeperData.Core.Ingestion.Models;

namespace KeeperData.Core.Ingestion.Contracts;

/// <summary>Picks the right normaliser for a dataset (PSV vs legacy H/C/D/T).</summary>
public interface INormaliserFactory
{
    INormaliser For(DataSetDefinition dataset);
}
