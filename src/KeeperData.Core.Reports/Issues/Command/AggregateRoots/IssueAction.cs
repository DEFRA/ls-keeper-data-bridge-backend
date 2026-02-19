namespace KeeperData.Core.Reports.Issues.Command.AggregateRoots;

/// <summary>
/// The type of action recorded in issue history.
/// </summary>
public enum IssueAction
{
    Created,
    Reactivated,
    Deactivated,
    Touched,
    Ignored,
    Unignored,
    ResolutionStatusChanged,
    Assigned,
    Unassigned
}
