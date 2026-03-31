namespace KeeperData.Core.Reports.Operations;

/// <summary>
/// Well-known status values for operation tree nodes.
/// </summary>
public static class OperationStatuses
{
    public const string NotStarted = "not-started";
    public const string InProgress = "in-progress";
    public const string Completed = "completed";
    public const string Failed = "failed";
    public const string Cancelled = "cancelled";
}
