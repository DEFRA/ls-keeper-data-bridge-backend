namespace KeeperData.Core.Reports.Dtos;

/// <summary>
/// Result of regenerating a presigned URL for a cleanse report.
/// </summary>
public record RegenerateReportUrlResult
{
    /// <summary>
    /// Gets whether the regeneration was successful.
    /// </summary>
    public required bool Success { get; init; }

    /// <summary>
    /// Gets the operation ID.
    /// </summary>
    public string? OperationId { get; init; }

    /// <summary>
    /// Gets the new presigned URL.
    /// </summary>
    public string? ReportUrl { get; init; }

    /// <summary>
    /// Gets the S3 object key.
    /// </summary>
    public string? ObjectKey { get; init; }

    /// <summary>
    /// Gets an error message if the operation failed.
    /// </summary>
    public string? Error { get; init; }
}
