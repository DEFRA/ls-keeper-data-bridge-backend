namespace KeeperData.Core.Reports.Issues.Command.AggregateRoots;

/// <summary>
/// Tracks the manual resolution workflow status of an issue.
/// </summary>
public enum ResolutionStatus
{
    None,
    Todo,
    InProgress,
    Resolved
}
