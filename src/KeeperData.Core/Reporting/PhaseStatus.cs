namespace KeeperData.Core.Reporting;

/// <summary>
/// Represents the status of an import phase (acquisition or ingestion).
/// </summary>
public enum PhaseStatus
{
    /// <summary>
    /// The phase has not yet started.
    /// </summary>
    NotStarted,

    /// <summary>
    /// The phase is currently running.
    /// </summary>
    Started,

    /// <summary>
    /// The phase has completed successfully.
    /// </summary>
    Completed,

    /// <summary>
    /// The phase failed with an error.
    /// </summary>
    Failed
}