namespace KeeperData.Core.Reports.Abstract;

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

/// <summary>
/// Service for exporting cleanse reports to CSV and uploading to S3.
/// </summary>
public interface ICleanseReportExportService
{
    /// <summary>
    /// Exports all active issues to a CSV file, zips it, uploads to S3, and returns a presigned URL.
    /// </summary>
    /// <param name="operationId">The operation ID for naming the report.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The export result containing the presigned URL.</returns>
    Task<CleanseReportExportResult> ExportAndUploadAsync(string operationId, CancellationToken ct = default);
}
