namespace KeeperData.Core.Reports.Querying;

/// <summary>
/// Specifies the field to sort issues by.
/// </summary>
public enum CleanseIssueSortField
{
    /// <summary>Sort by CPH value.</summary>
    Cph,

    /// <summary>Sort by issue code.</summary>
    IssueCode,

    /// <summary>Sort by creation timestamp.</summary>
    CreatedAtUtc,

    /// <summary>Sort by last update timestamp.</summary>
    LastUpdatedAtUtc
}
