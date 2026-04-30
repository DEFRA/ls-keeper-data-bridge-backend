namespace KeeperData.Core.ETL.Impl;

public static class StandardDataSetDefinitionsBuilder
{
    public static DataSetDefinitions Build()
    {
        var list = new List<DataSetDefinition>();
        var samCPHHolding = list.With(new DataSetDefinition("sam_cph_holdings", "LITP_SAMCPHHOLDING_{0}", ["CPH", "FEATURE_NAME", "SECONDARY_CPH", "ANIMAL_SPECIES_CODE"], ChangeType.HeaderName, []));
        var ctscphHolding = list.With(new DataSetDefinition("cts_cph_holding", "LITP_CTSCPHHOLDING_{0}", ["LID_FULL_IDENTIFIER"], ChangeType.HeaderName, []));
        var ctsKeeper = list.With(new DataSetDefinition("cts_keeper", "LITP_CTSKEEPER_{0}", ["PAR_ID", "LID_FULL_IDENTIFIER"], ChangeType.HeaderName, []));
        var samCPHHolder = list.With(new DataSetDefinition("sam_cph_holder", "LITP_SAMCPHHOLDER_{0}", ["PARTY_ID"], ChangeType.HeaderName, []));
        var samHerd = list.With(new DataSetDefinition("sam_herd", "LITP_SAMHERD_{0}", ["CPHH", "HERDMARK", "ANIMAL_PURPOSE_CODE"], ChangeType.HeaderName, []));
        var samParty = list.With(new DataSetDefinition("sam_party", "LITP_SAMPARTY_{0}", ["PARTY_ID"], ChangeType.HeaderName, []));

        var samTla = list.With(new DataSetDefinition("sam_tla", "LITP_SAMTLA_{0}", ["TEMP_CPH", "TEMP_LAND_OS_MAP_REFERENCE", "PERMANENT_CPH"], ChangeType.HeaderName, []));
        var amls2CommonLand = list.With(new DataSetDefinition("amls2_common_land", "LITP_AMLS2COMMONLAND_{0}", ["MAIN_CPH", "COMMON_CPH"], ChangeType.HeaderName, []));
        var amls2Port = list.With(new DataSetDefinition("amls2_port", "LITP_AMLS2PORT_{0}", ["CPH"], ChangeType.HeaderName, []));
        var ctsAgent = list.With(new DataSetDefinition("cts_agent", "LITP_CTSAGENT_{0}", ["PAR_ID", "LID_FULL_IDENTIFIER"], ChangeType.HeaderName, []));
        var amesHaulier = list.With(new DataSetDefinition("ames_haulier", "LITP_AMESHAULIER_{0}", ["DISPLAY_LICENCE_NUMBER"], ChangeType.HeaderName, [])); // no PK defined in the spec, but 'DISPLAY_LICENCE_NUMBER' is the only unique field in the data - and field cannot be null.
        var samShowground = list.With(new DataSetDefinition("sam_showground", "LITP_SAMSHOWGROUND_{0}", ["CPH"], ChangeType.HeaderName, []));

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

    private static T With<T>(this List<T> list, T item)
    {
        list.Add(item);
        return item;
    }
}