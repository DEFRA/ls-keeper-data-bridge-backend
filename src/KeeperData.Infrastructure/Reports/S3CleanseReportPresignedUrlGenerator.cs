using Amazon.S3;
using Amazon.S3.Model;
using KeeperData.Core.Reports.Abstract;

namespace KeeperData.Infrastructure.Reports;

/// <summary>
/// S3 implementation of presigned URL generation for cleanse reports.
/// </summary>
public class S3CleanseReportPresignedUrlGenerator : ICleanseReportPresignedUrlGenerator
{
    private static readonly TimeSpan DefaultExpiry = TimeSpan.FromDays(7);

    private readonly IAmazonS3 _s3Client;
    private readonly string _bucketName;
    private readonly string? _topLevelFolder;

    public S3CleanseReportPresignedUrlGenerator(IAmazonS3 s3Client, string bucketName, string? topLevelFolder = null)
    {
        _s3Client = s3Client;
        _bucketName = bucketName;
        _topLevelFolder = NormalizeTopLevelFolder(topLevelFolder);
    }

    /// <inheritdoc />
    public string GeneratePresignedUrl(string objectKey, TimeSpan? expiresIn = null)
    {
        var fullKey = GetFullObjectKey(objectKey);
        var expiry = expiresIn ?? DefaultExpiry;

        var request = new GetPreSignedUrlRequest
        {
            BucketName = _bucketName,
            Key = fullKey,
            Verb = HttpVerb.GET,
            Expires = DateTime.UtcNow.Add(expiry)
        };

        return _s3Client.GetPreSignedURL(request);
    }

    private string GetFullObjectKey(string objectKey)
    {
        if (string.IsNullOrEmpty(_topLevelFolder))
            return objectKey;

        return $"{_topLevelFolder}{objectKey.TrimStart('/')}";
    }

    private static string? NormalizeTopLevelFolder(string? topLevelFolder)
    {
        if (string.IsNullOrWhiteSpace(topLevelFolder))
            return null;

        // Ensure it ends with a slash
        return topLevelFolder.TrimEnd('/') + "/";
    }
}
