namespace KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;

/// <summary>
/// Represents the distinct phases of a cleanse analysis operation.
/// </summary>
public enum OperationPhase
{
    /// <summary>
    /// Phase 1: Analyse CTS and SAM holdings to detect issues.
    /// </summary>
    Analysis,

    /// <summary>
    /// Phase 2: Deactivate stale issues not touched by this operation.
    /// </summary>
    Deactivation,

    /// <summary>
    /// Phase 3: Export report to CSV, zip, upload to S3, and send notification.
    /// </summary>
    Export
}
