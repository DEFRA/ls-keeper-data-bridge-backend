namespace KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;

/// <summary>
/// Represents the status of a cleanse analysis operation.
/// </summary>
public enum CleanseAnalysisStatus
{
    /// <summary>
    /// The operation has not started yet.
    /// </summary>
    NotStarted,

    /// <summary>
    /// The operation is currently running.
    /// </summary>
    Running,

    /// <summary>
    /// The operation completed successfully.
    /// </summary>
    Completed,

    /// <summary>
    /// The operation failed with an error.
    /// </summary>
    Failed,

    /// <summary>
    /// Cancellation has been requested but the operation has not yet stopped.
    /// The pump will detect this at the next progress checkpoint and transition to Cancelled.
    /// </summary>
    Cancelling,

    /// <summary>
    /// The operation was cancelled.
    /// </summary>
    Cancelled
}
