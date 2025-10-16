using KeeperData.Core.ETL.Abstract;
using System.Collections.Immutable;

namespace KeeperData.Core.ETL.Impl;


public record DataSetDefinition(string Name, string FilePrefixFormat, string DatePattern, string PrimaryKeyHeaderName, string ChangeTypeHeaderName);

public class DataSetDefinitions : IDataSetDefinitions
{
    public required DataSetDefinition SamCPHHolding { get; init; }

    public ImmutableArray<DataSetDefinition> All { get; init; }

}