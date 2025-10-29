using KeeperData.Core.ETL.Impl;
using System.Collections.Immutable;

namespace KeeperData.Core.ETL.Abstract;

public interface IDataSetDefinitions
{
    ImmutableArray<DataSetDefinition> All { get; }
}