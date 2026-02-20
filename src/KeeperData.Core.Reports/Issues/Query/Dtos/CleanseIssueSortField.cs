namespace KeeperData.Core.Reports.Issues.Query.Dtos;

/// <summary>
/// Specifies the field to sort issues by.
/// </summary>
public enum CleanseIssueSortField
{
    /// <summary>Sort by CPH value.</summary>
    Cph,

    /// <summary>Sort by CTS LID full identifier.</summary>
    CtsLidFullIdentifier,

    /// <summary>Sort by issue code.</summary>
    IssueCode,

    /// <summary>Sort by rule code.</summary>
    RuleCode,

    /// <summary>Sort by error code.</summary>
    ErrorCode,

    /// <summary>Sort by creation timestamp.</summary>
    CreatedAtUtc,

    /// <summary>Sort by last update timestamp.</summary>
    LastUpdatedAtUtc,

    /// <summary>Sort by active status.</summary>
    IsActive,

    /// <summary>Sort by ignored status.</summary>
    IsIgnored,

    /// <summary>Sort by resolution status.</summary>
    ResolutionStatus,

    /// <summary>Sort by assigned user.</summary>
    AssignedTo
}
