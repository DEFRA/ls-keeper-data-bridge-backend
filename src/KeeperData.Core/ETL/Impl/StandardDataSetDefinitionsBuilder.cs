namespace KeeperData.Core.ETL.Impl;

public static class StandardDataSetDefinitionsBuilder
{
    public static DataSetDefinitions Build()
    {
        var list = new List<DataSetDefinition>();
        var samCPHHolding = list.With(new DataSetDefinition("sam_cph_holdings", "LITP_SAMCPHHOLDING_{0}", ["CPH"], ChangeType.HeaderName,
        [
            "ADDRESS_PK",
            "DISEASE_TYPE",
            "INTERVAL",
            "INTERVAL_UNIT_OF_TIME",
            "CPH_RELATIONSHIP_TYPE",
            "SECONDARY_CPH",
            "ANIMAL_SPECIES_CODE",
            "ANIMAL_PRODUCTION_USAGE_CODE",
        ]));

        var ctscphHolding = list.With(new DataSetDefinition("cts_cph_holding", "LITP_CTSCPHHOLDING_{0}", ["LID_FULL_IDENTIFIER"], ChangeType.HeaderName, []));
        var ctsKeeper = list.With(new DataSetDefinition("cts_keeper", "LITP_CTSKEEPER_{0}", ["PAR_ID"], ChangeType.HeaderName, []));
        var samCPHHolder = list.With(new DataSetDefinition("sam_cph_holder", "LITP_SAMCPHHOLDER_{0}", ["PARTY_ID"], ChangeType.HeaderName, []));
        var samHerd = list.With(new DataSetDefinition("sam_herd", "LITP_SAMHERD_{0}", ["CPHH", "HERDMARK"], ChangeType.HeaderName, []));
        var samParty = list.With(new DataSetDefinition("sam_party", "LITP_SAMPARTY_{0}", ["PARTY_ID"], ChangeType.HeaderName, []));

        return new DataSetDefinitions
        {
            SamCPHHolding = samCPHHolding,
            CTSCPHHolding = ctscphHolding,
            CTSKeeper = ctsKeeper,
            SamCPHHolder = samCPHHolder,
            SamHerd = samHerd,
            SamParty = samParty,
            All = [.. list]
        };
    }

    private static T With<T>(this List<T> list, T item)
    {
        list.Add(item);
        return item;
    }
}