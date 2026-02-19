namespace KeeperData.Core.Reports.Cleanse.Export.Command.Results;

/// <summary>
/// Result of a cleanse report export operation.
/// </summary>
public record CleanseReportExportResult
{
    /// <summary>
    /// Gets whether the export was successful.
    /// </summary>
    public required bool Success { get; init; }

    /// <summary>
    /// Gets the presigned URL to download the report.
    /// </summary>
    public string? ReportUrl { get; init; }

    /// <summary>
    /// Gets the S3 object key where the report was uploaded.
    /// </summary>
    public string? ObjectKey { get; init; }

    /// <summary>
    /// Gets the error message if the export failed.
    /// </summary>
    public string? Error { get; init; }
}
