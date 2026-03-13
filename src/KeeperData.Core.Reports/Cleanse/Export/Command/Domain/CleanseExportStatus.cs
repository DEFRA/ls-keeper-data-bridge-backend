namespace KeeperData.Core.Reports.Cleanse.Export.Command.Domain;

/// <summary>
/// Represents the status of an ad-hoc cleanse export operation.
/// </summary>
public enum CleanseExportStatus
{
    /// <summary>The export has been accepted but not yet started.</summary>
    Pending,

    /// <summary>The export is currently running.</summary>
    Running,

    /// <summary>The export completed successfully.</summary>
    Completed,

    /// <summary>The export failed.</summary>
    Failed
}
