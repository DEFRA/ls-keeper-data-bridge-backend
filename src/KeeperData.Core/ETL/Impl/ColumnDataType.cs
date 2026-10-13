namespace KeeperData.Core.ETL.Impl;

/// <summary>The parquet column type the optimise stage converts a string column to.</summary>
public enum ColumnDataType
{
    String = 0,
    Int64 = 1,
    Double = 2,
    Decimal = 3,
    Boolean = 4,
    Date = 5,
    Timestamp = 6
}
