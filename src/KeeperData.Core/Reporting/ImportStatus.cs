namespace KeeperData.Core.Reporting;

/// <summary>
/// Represents the status of an import operation.
/// </summary>
public enum ImportStatus
{
    /// <summary>
    /// The import has been started and is currently running.
    /// </summary>
    Started,

    /// <summary>
    /// The import has completed successfully.
    /// </summary>
    Completed,

    /// <summary>
    /// The import failed with an error.
    /// </summary>
    Failed
}