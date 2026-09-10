namespace KeeperData.Core.ETL.Impl;

/// <summary>
/// The datasets the bridge ingests. The external storage service is scoped to the bucket root so
/// that every source folder is reachable, so a definition's prefix names the folder its files live
/// in as well as the file name itself. The litprd folder comes from
/// StorageConfiguration:SourceExternalDataSetFolder.
/// </summary>
public static class StandardDataSetDefinitionsBuilder
{
    /// <summary>The folder the litprd feed drops its files in, absent configuration saying otherwise.</summary>
    public const string DefaultDataSetFolder = "litprd";

    /// <param name="dataSetFolder">The folder the litprd feed drops its files in, relative to the
    /// bucket root the external storage service is scoped to. Null takes
    /// <see cref="DefaultDataSetFolder"/>; empty puts the files at the root.</param>
    public static DataSetDefinitions Build(string? dataSetFolder = null)
    {
        var litprd = FolderPrefix(dataSetFolder ?? DefaultDataSetFolder);

        var list = new List<DataSetDefinition>();
        var samCPHHolding = list.With(new DataSetDefinition("sam_cph_holdings", $"{litprd}LITP_SAMCPHHOLDING_{{0}}", ["CPH", "FEATURE_NAME", "SECONDARY_CPH", "ANIMAL_SPECIES_CODE"], ChangeType.HeaderName, [], IngestionMode: DataSetIngestionMode.Delta));
        var ctscphHolding = list.With(new DataSetDefinition("cts_cph_holding", $"{litprd}LITP_CTSCPHHOLDING_{{0}}", ["LID_FULL_IDENTIFIER"], ChangeType.HeaderName, [], IngestionMode: DataSetIngestionMode.Delta));
        var ctsKeeper = list.With(new DataSetDefinition("cts_keeper", $"{litprd}LITP_CTSKEEPER_{{0}}", ["PAR_ID", "LID_FULL_IDENTIFIER"], ChangeType.HeaderName, [], IngestionMode: DataSetIngestionMode.Delta));
        var samCPHHolder = list.With(new DataSetDefinition("sam_cph_holder", $"{litprd}LITP_SAMCPHHOLDER_{{0}}", ["PARTY_ID"], ChangeType.HeaderName, [], IngestionMode: DataSetIngestionMode.Delta));
        var samHerd = list.With(new DataSetDefinition("sam_herd", $"{litprd}LITP_SAMHERD_{{0}}", ["CPHH", "HERDMARK", "ANIMAL_PURPOSE_CODE"], ChangeType.HeaderName, [], IngestionMode: DataSetIngestionMode.Delta));
        var samParty = list.With(new DataSetDefinition("sam_party", $"{litprd}LITP_SAMPARTY_{{0}}", ["PARTY_ID"], ChangeType.HeaderName, [], IngestionMode: DataSetIngestionMode.Delta));

        var samTla = list.With(new DataSetDefinition("sam_tla", $"{litprd}LITP_SAMTLA_{{0}}", ["TEMP_CPH", "TEMP_LAND_OS_MAP_REFERENCE", "PERMANENT_CPH"], ChangeType.HeaderName, [], IngestionMode: DataSetIngestionMode.Delta));
        var amls2CommonLand = list.With(new DataSetDefinition("amls2_common_land", $"{litprd}LITP_AMLS2COMMONLAND_{{0}}", ["MAIN_CPH", "COMMON_CPH"], ChangeType.HeaderName, [], IngestionMode: DataSetIngestionMode.Delta));
        var amls2Port = list.With(new DataSetDefinition("amls2_port", $"{litprd}LITP_AMLS2PORT_{{0}}", ["CPH"], ChangeType.HeaderName, [], IngestionMode: DataSetIngestionMode.Delta));
        var ctsAgent = list.With(new DataSetDefinition("cts_agent", $"{litprd}LITP_CTSAGENT_{{0}}", ["PAR_ID", "LID_FULL_IDENTIFIER"], ChangeType.HeaderName, [], IngestionMode: DataSetIngestionMode.Delta));
        var amesHaulier = list.With(new DataSetDefinition("ames_haulier", $"{litprd}LITP_AMESHAULIER_{{0}}", ["DISPLAY_LICENCE_NUMBER"], ChangeType.HeaderName, [], IngestionMode: DataSetIngestionMode.Delta)); // no PK defined in the spec, but 'DISPLAY_LICENCE_NUMBER' is the only unique field in the data - and field cannot be null.
        var samShowground = list.With(new DataSetDefinition("sam_showground", $"{litprd}LITP_SAMSHOWGROUND_{{0}}", ["CPH"], ChangeType.HeaderName, [], IngestionMode: DataSetIngestionMode.Delta));

        var ctsLocationIdentifiers = list.With(Cts("cts_location_identifiers", "CT_LOCATION_IDENTIFIERS", "LID"));
        var ctsLocations = list.With(Cts("cts_locations", "CT_LOCATIONS", "LOC"));
        var ctsLocationPartyRels = list.With(Cts("cts_location_party_rels", "CT_LOCATION_PARTY_RELS", "LPR"));
        var ctsParties = list.With(Cts("cts_parties", "CT_PARTIES", "PAR"));
        var ctsAddresses = list.With(Cts("cts_addresses", "CT_ADDRESSES", "ADR"));
        var ctsCounties = list.With(Cts("cts_counties", "CT_COUNTIES", "CTY"));

        return new DataSetDefinitions
        {
            SamCPHHolding = samCPHHolding,
            CTSCPHHolding = ctscphHolding,
            CTSKeeper = ctsKeeper,
            SamCPHHolder = samCPHHolder,
            SamHerd = samHerd,
            SamParty = samParty,

            SamTla = samTla,
            Amls2CommonLand = amls2CommonLand,
            Amls2Port = amls2Port,
            CtsAgent = ctsAgent,
            AmesHaulier = amesHaulier,
            SamShowground = samShowground,
            CtsLocationIdentifiers = ctsLocationIdentifiers,
            CtsDataSets =
            [
                ctsLocationIdentifiers,
                ctsLocations,
                ctsLocationPartyRels,
                ctsParties,
                ctsAddresses,
                ctsCounties
            ],
            All = [.. list]
        };
    }

    /// <summary>
    /// A CTS table, which the feed names to a fixed shape:
    /// <c>CTSM_CADS_&lt;environment&gt;_&lt;BULK|DELTA&gt;_&lt;run&gt;_&lt;part&gt;_&lt;table&gt;_&lt;yyyy-MM-dd-HHmmss&gt;</c>,
    /// with every column of the table prefixed by the same three letters. So a dataset is described by
    /// its source table and that prefix, and nothing else about it is per-dataset.
    ///
    /// The environment segment is a wildcard: the same table is published as PREP by preprod and PROD by
    /// production, and it is the file that says which, not us. Discovery reads SourceKeyPattern; the
    /// prefix format is kept populated for the callers that report it. The patterns carry no extension
    /// because a file is matched both in the source lane, where it may be .csv, .csv.enc or .xsvn.csv,
    /// and in the normalised lane, where it is .parquet. The baseline discriminates on the file name
    /// rather than on the folder, which a normalised key drops: both lanes name the run that produced
    /// the file, and a baseline run is a _BULK_ one. The table is anchored between underscores so that a
    /// dataset never captures a sibling table whose name extends its own.
    /// </summary>
    /// <param name="name">The dataset name, which is also its staging table name.</param>
    /// <param name="table">The CTS source table, as it appears in every file name.</param>
    /// <param name="columnPrefix">The three letters the table's own columns are prefixed by.</param>
    private static DataSetDefinition Cts(string name, string table, string columnPrefix)
        => new(
            name,
            $"cads/cts/**/CTSM_CADS_*_*_{table}_{{0}}",
            [$"{columnPrefix}_ID"],
            $"{columnPrefix}_AUD_TYPE",
            [],
            DateTimePattern: "yyyy-MM-dd-HHmmss",
            Format: FileFormat.Hcdt,
            IngestionMode: DataSetIngestionMode.Delta,
            PasswordDerivation: PasswordDerivationPolicy.CtsDerived,
            SourceKeyPattern: $"cads/cts/{{bulk,daily}}/*_{table}_*",
            BaselineKeyPattern: $"cads/cts/bulk/*_BULK_*_{table}_*",
            Audit: new AuditColumns($"{columnPrefix}_AUD_ID", $"{columnPrefix}_AUD_DATETIME"))
        {
            ExcludedColumns =
            [
                $"{columnPrefix}_AUD_ID",
                $"{columnPrefix}_AUD_TYPE",
                $"{columnPrefix}_AUD_DATETIME",
                "RECORD_TYPE",
                "RECORD_COUNT"
            ]
        };

    private static string FolderPrefix(string folder)
    {
        var trimmed = folder.Trim().Trim('/');
        return trimmed.Length == 0 ? string.Empty : $"{trimmed}/";
    }

    private static T With<T>(this List<T> list, T item)
    {
        list.Add(item);
        return item;
    }
}