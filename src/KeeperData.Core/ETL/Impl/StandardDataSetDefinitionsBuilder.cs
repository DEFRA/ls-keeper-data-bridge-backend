namespace KeeperData.Core.ETL.Impl;

public static class StandardDataSetDefinitionsBuilder
{
    public const string DatePattern = "yyyyMMdd";
    public const string DateTimePattern = DatePattern + "HHmmss";

    public static DataSetDefinitions Build()
    {
        var list = new List<DataSetDefinition>();
        var samCPHHolding = list.With(new DataSetDefinition("sam_cph_holdings", "LITP_SAMCPHHOLDING_{0}", DatePattern, "CPH", ChangeType.HeaderName));

        return new DataSetDefinitions
        {
            SamCPHHolding = samCPHHolding,
            All = [.. list]
        };
    }

    private static T With<T>(this List<T> list, T item)
    {
        list.Add(item);
        return item;
    }
}