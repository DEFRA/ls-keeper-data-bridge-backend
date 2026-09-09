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

/// <summary>The extra columns a delta file carries to describe the change rather than the row.
/// The change type itself is the definition's ChangeTypeHeaderName.</summary>
public sealed record AuditColumns(string SequenceColumn, string TimestampColumn);

/// <param name="SourceKeyPattern">Glob matching every file of the dataset. Null discovers by
/// <paramref name="FilePrefixFormat"/> instead.</param>
/// <param name="BaselineKeyPattern">Glob matching only the baseline lane of a dataset whose files
/// arrive as a baseline plus deltas.</param>
/// <param name="Audit">Present for a dataset whose deltas describe their own ordering, and so are
/// merged in audit order rather than file order.</param>
public record DataSetDefinition(string Name, string FilePrefixFormat, string[] PrimaryKeyHeaderNames, string ChangeTypeHeaderName, string[] Accumulators, string DatePattern = EtlConstants.DatePattern, string DateTimePattern = EtlConstants.DateTimePattern, FileFormat Format = FileFormat.SimplePsv, DataSetIngestionMode IngestionMode = DataSetIngestionMode.Snapshot, PasswordDerivationPolicy PasswordDerivation = PasswordDerivationPolicy.FileNameVerbatim, string? SourceKeyPattern = null, string? BaselineKeyPattern = null, AuditColumns? Audit = null)
{
    /// <summary>Columns the snapshot suppresses. An empty array keeps every column.</summary>
    public string[] ExcludedColumns { get; init; } = [];
}

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

    public DataSetDefinition? CtsLocationIdentifiers { get; init; }


    public ImmutableArray<DataSetDefinition> All { get; init; }

}
