using KeeperData.Core.ETL.Abstract;
using System.Collections.Immutable;

namespace KeeperData.Core.ETL.Impl;


public enum FileFormat
{
    SimplePsv,
    Hcdt
}

/// <summary>How a source file's decryption password is obtained from its name.</summary>
public enum PasswordDerivationPolicy
{
    /// <summary>The file name is the password.</summary>
    FileNameVerbatim = 0,

    /// <summary>CTS encodes the password in the name: the date from the trailing
    /// yyyy-MM-dd-HHmmss timestamp, then every preceding underscore-separated segment reversed.</summary>
    CtsDerived = 1
}

public record DataSetDefinition(string Name, string FilePrefixFormat, string[] PrimaryKeyHeaderNames, string ChangeTypeHeaderName, string[] Accumulators, string DatePattern = EtlConstants.DatePattern, string DateTimePattern = EtlConstants.DateTimePattern, FileFormat Format = FileFormat.SimplePsv, DataSetIngestionMode IngestionMode = DataSetIngestionMode.Snapshot, PasswordDerivationPolicy PasswordDerivation = PasswordDerivationPolicy.FileNameVerbatim);

public class DataSetDefinitions : IDataSetDefinitions
{
    public required DataSetDefinition SamCPHHolding { get; init; }

    public required DataSetDefinition CTSCPHHolding { get; init; }

    public required DataSetDefinition CTSKeeper { get; init; }

    public required DataSetDefinition SamCPHHolder { get; init; }

    public required DataSetDefinition SamHerd { get; init; }

    public required DataSetDefinition SamParty { get; init; }


    public required DataSetDefinition SamTla { get; init; }
    
    public required DataSetDefinition Amls2CommonLand { get; init; }

    public required DataSetDefinition Amls2Port { get; init; }

    public required DataSetDefinition CtsAgent { get; init; }

    public required DataSetDefinition AmesHaulier { get; init; }

    public required DataSetDefinition SamShowground { get; init; }


    public ImmutableArray<DataSetDefinition> All { get; init; }

}
