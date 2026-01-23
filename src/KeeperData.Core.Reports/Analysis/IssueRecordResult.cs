namespace KeeperData.Core.Reports.Analysis;

/// <summary>
/// Result of recording or resolving an issue.
/// </summary>
public enum IssueRecordResult
{
    /// <summary>
    /// A new issue was created.
    /// </summary>
    Created,

    /// <summary>
    /// An existing inactive issue was reactivated.
    /// </summary>
    Reactivated,

    /// <summary>
    /// An active issue was resolved (deactivated).
    /// </summary>
    Resolved,

    /// <summary>
    /// No change was made (issue didn't exist or was already in the target state).
    /// </summary>
    NoChange
}
