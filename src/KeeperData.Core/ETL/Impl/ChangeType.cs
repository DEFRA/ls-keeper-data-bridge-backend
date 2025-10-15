namespace KeeperData.Core.ETL.Impl;

/// <summary>
/// Constants for CSV change type operations used in data ingestion.
/// </summary>
public static class ChangeType
{
    /// <summary>
    /// The header name for the change type column in CSV files.
    /// </summary>
    public const string HeaderName = "CHANGE_TYPE";

    /// <summary>
    /// Insert operation - creates a new record or updates an existing active record.
    /// </summary>
    public const string Insert = "I";

    /// <summary>
    /// Update operation - updates an existing active record.
    /// </summary>
    public const string Update = "U";

    /// <summary>
    /// Delete operation - soft deletes a record by marking it as deleted.
    /// </summary>
    public const string Delete = "D";
}
