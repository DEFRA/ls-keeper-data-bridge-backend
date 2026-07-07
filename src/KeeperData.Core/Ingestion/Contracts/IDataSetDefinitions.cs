using KeeperData.Core.Ingestion.Models;

namespace KeeperData.Core.Ingestion.Contracts;

/// <summary>All dataset definitions the run processes.</summary>
public interface IDataSetDefinitions
{
    IReadOnlyList<DataSetDefinition> All { get; }
}
