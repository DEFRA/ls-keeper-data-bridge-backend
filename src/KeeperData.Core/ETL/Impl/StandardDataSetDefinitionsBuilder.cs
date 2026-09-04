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
            
            All = [.. list]
        };
    }

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