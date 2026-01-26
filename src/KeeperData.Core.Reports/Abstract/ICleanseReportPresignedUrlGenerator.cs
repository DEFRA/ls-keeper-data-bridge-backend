namespace KeeperData.Core.Reports.Abstract;

/// <summary>
/// Generates presigned URLs for cleanse report downloads.
/// </summary>
public interface ICleanseReportPresignedUrlGenerator
{
    /// <summary>
    /// Generates a presigned URL for downloading a cleanse report.
    /// </summary>
    /// <param name="objectKey">The S3 object key.</param>
    /// <param name="expiresIn">Optional expiry duration. Defaults to 7 days if not specified.</param>
    /// <returns>A presigned URL for downloading the object.</returns>
    string GeneratePresignedUrl(string objectKey, TimeSpan? expiresIn = null);
}
