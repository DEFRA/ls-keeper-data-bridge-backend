using KeeperData.Core.ETL.Abstract;
using System.Collections.Immutable;

namespace KeeperData.Core.ETL.Impl;


public record DataSetDefinition(string Name, string FilePrefixFormat, string DatePattern, string[] PrimaryKeyHeaderNames, string ChangeTypeHeaderName, string[] Accumulators);

public class DataSetDefinitions : IDataSetDefinitions
{
    public required DataSetDefinition SamCPHHolding { get; init; }

    public required DataSetDefinition CTSCPHHolding { get; init; }

    public required DataSetDefinition CTSKeeper { get; init; }

    public required DataSetDefinition SamCPHHolder { get; init; }

    public required DataSetDefinition SamHerd { get; init; }

    public required DataSetDefinition SamParty { get; init; }

    public ImmutableArray<DataSetDefinition> All { get; init; }

}