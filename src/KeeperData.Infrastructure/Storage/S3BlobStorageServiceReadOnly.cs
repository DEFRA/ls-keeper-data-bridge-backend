using Amazon.S3;
using Amazon.S3.Model;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using Microsoft.Extensions.Logging;
using System.Linq;

namespace KeeperData.Infrastructure.Storage;

public class S3BlobStorageServiceReadOnly : IBlobStorageServiceReadOnly, IDisposable
{
    protected readonly IAmazonS3 _s3Client;
    protected readonly ILogger _logger;
    protected readonly string _bucketName;
    protected readonly string? _topLevelFolder;
    private readonly bool _shouldDisposeClient;

    public S3BlobStorageServiceReadOnly(
        IAmazonS3 s3Client,
        ILogger logger,
        string bucketName,
        string? topLevelFolder = null)
    {
        _s3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
        _topLevelFolder = NormalizeTopLevelFolder(topLevelFolder);
        _shouldDisposeClient = false;
    }

    public S3BlobStorageServiceReadOnly(
        AmazonS3Config config,
        ILogger logger,
        string container,
        string? topLevelFolder = null)
        : this(new AmazonS3Client(config), logger, container, topLevelFolder, shouldDisposeClient: true)
    {
    }

    public S3BlobStorageServiceReadOnly(
        string accessKey,
        string secretKey,
        AmazonS3Config config,
        ILogger logger,
        string container,
        string? topLevelFolder = null)
        : this(new AmazonS3Client(accessKey, secretKey, config), logger, container, topLevelFolder, shouldDisposeClient: true)
    {
    }

    public S3BlobStorageServiceReadOnly(
        string accessKey,
        string secretKey,
        string sessionToken,
        AmazonS3Config config,
        ILogger logger,
        string container,
        string? topLevelFolder = null)
        : this(new AmazonS3Client(accessKey, secretKey, sessionToken, config), logger, container, topLevelFolder, shouldDisposeClient: true)
    {
    }

    public S3BlobStorageServiceReadOnly(
        string serviceUrl,
        string accessKey,
        string secretKey,
        ILogger logger,
        string container,
        string? topLevelFolder = null)
        : this(accessKey, secretKey, new AmazonS3Config
        {
            ServiceURL = serviceUrl,
            ForcePathStyle = true
        }, logger, container, topLevelFolder)
    {
    }

    public S3BlobStorageServiceReadOnly(
        string profileName,
        Amazon.RegionEndpoint region,
        ILogger logger,
        string container,
        string? topLevelFolder = null)
        : this(new AmazonS3Client(region), logger, container, topLevelFolder, shouldDisposeClient: true)
    {
    }

    public S3BlobStorageServiceReadOnly(
        Amazon.RegionEndpoint region,
        ILogger logger,
        string container,
        string? topLevelFolder = null)
        : this(new AmazonS3Client(region), logger, container, topLevelFolder, shouldDisposeClient: true)
    {
    }

    private S3BlobStorageServiceReadOnly(
        IAmazonS3 s3Client,
        ILogger logger,
        string container,
        string? topLevelFolder,
        bool shouldDisposeClient)
    {
        _s3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _bucketName = container ?? throw new ArgumentNullException(nameof(container));
        _topLevelFolder = NormalizeTopLevelFolder(topLevelFolder);
        _shouldDisposeClient = shouldDisposeClient;
    }

    protected string GetFullObjectKey(string objectKey)
    {
        if (string.IsNullOrEmpty(_topLevelFolder))
            return objectKey;

        return $"{_topLevelFolder}{objectKey.TrimStart('/')}";
    }

    private string GetRelativeObjectKey(string fullObjectKey)
    {
        if (string.IsNullOrEmpty(_topLevelFolder))
            return fullObjectKey;

        if (fullObjectKey.StartsWith(_topLevelFolder, StringComparison.Ordinal))
            return fullObjectKey.Substring(_topLevelFolder.Length);

        return fullObjectKey;
    }

    private string? GetFullPrefix(string? prefix)
    {
        if (string.IsNullOrEmpty(_topLevelFolder))
            return prefix;

        if (string.IsNullOrEmpty(prefix))
            return _topLevelFolder.TrimEnd('/');

        return $"{_topLevelFolder}{prefix.TrimStart('/')}";
    }

    public async Task<IReadOnlyList<StorageObjectInfo>> ListAsync(
        string? prefix = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var objects = new List<StorageObjectInfo>();
            var continuationToken = (string?)null;

            do
            {
                var page = await ListPageAsync(prefix, 1000, continuationToken, cancellationToken).ConfigureAwait(false);
                objects.AddRange(page.Items);
                continuationToken = page.ContinuationToken;
            }
            while (!string.IsNullOrEmpty(continuationToken));

            return objects;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to list objects in container {Container} with prefix {Prefix}", _bucketName, prefix);
            throw;
        }
    }

    public async Task<StorageListPage> ListPageAsync(
        string? prefix = null,
        int pageSize = 1000,
        string? continuationToken = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var request = new ListObjectsV2Request
            {
                BucketName = _bucketName,
                Prefix = GetFullPrefix(prefix),
                MaxKeys = Math.Min(Math.Max(pageSize, 1), 1000),
                ContinuationToken = continuationToken
            };

            var response = await _s3Client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);

            var items = (response.S3Objects ?? Enumerable.Empty<S3Object>()).Select(obj => new StorageObjectInfo
            {
                Container = _bucketName,
                Key = GetRelativeObjectKey(obj.Key),
                Size = obj.Size ?? 0,
                LastModified = obj.LastModified ?? DateTimeOffset.MinValue,
                ETag = obj.ETag?.Trim('"'),
                StorageUri = new Uri($"s3://{_bucketName}/{obj.Key}"),
                HttpUri = _s3Client.GetPreSignedURL(new GetPreSignedUrlRequest
                {
                    BucketName = _bucketName,
                    Key = obj.Key,
                    Verb = HttpVerb.GET,
                    Expires = DateTime.UtcNow.AddHours(1)
                }) is string url ? new Uri(url) : null
            }).ToList();

            return new StorageListPage
            {
                Items = items,
                ContinuationToken = response.NextContinuationToken,
                IsTruncated = response.IsTruncated
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to list page of objects in container {Container} with prefix {Prefix}", _bucketName, prefix);
            throw;
        }
    }

    public async Task<StorageObjectMetadata> GetMetadataAsync(
        string objectKey,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var fullObjectKey = GetFullObjectKey(objectKey);
            var request = new GetObjectMetadataRequest
            {
                BucketName = _bucketName,
                Key = fullObjectKey
            };

            var response = await _s3Client.GetObjectMetadataAsync(request, cancellationToken).ConfigureAwait(false);

            return new StorageObjectMetadata
            {
                Container = _bucketName,
                Key = objectKey,
                ContentLength = response.ContentLength,
                ContentType = response.Headers.ContentType,
                ETag = response.ETag?.Trim('"'),
                LastModified = response.LastModified,
                StorageClass = response.StorageClass?.Value,
                Encryption = response.ServerSideEncryptionMethod?.Value,
                StorageUri = new Uri($"s3://{_bucketName}/{fullObjectKey}"),
                HttpUri = _s3Client.GetPreSignedURL(new GetPreSignedUrlRequest
                {
                    BucketName = _bucketName,
                    Key = fullObjectKey,
                    Verb = HttpVerb.GET,
                    Expires = DateTime.UtcNow.AddHours(1)
                }) is string url ? new Uri(url) : null,
                UserMetadata = response.Metadata?.Keys.ToDictionary(key => key, key => response.Metadata[key]) ??
                              new Dictionary<string, string>(),
                ProviderProperties = new Dictionary<string, string>
                {
                    ["StorageClass"] = response.StorageClass?.Value ?? "",
                    ["ServerSideEncryption"] = response.ServerSideEncryptionMethod?.Value ?? "",
                    ["VersionId"] = response.VersionId ?? ""
                }
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get metadata for object {ObjectKey} in container {Container}", objectKey, _bucketName);
            throw;
        }
    }

    public async Task<byte[]> DownloadAsync(
        string objectKey,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var fullObjectKey = GetFullObjectKey(objectKey);
            using var stream = await OpenReadAsync(objectKey, cancellationToken).ConfigureAwait(false);
            using var memoryStream = new MemoryStream();
            await stream.CopyToAsync(memoryStream, cancellationToken).ConfigureAwait(false);
            return memoryStream.ToArray();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to download object {ObjectKey} from container {Container}", objectKey, _bucketName);
            throw;
        }
    }

    public async Task<Stream> OpenReadAsync(
        string objectKey,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var fullObjectKey = GetFullObjectKey(objectKey);
            var request = new GetObjectRequest
            {
                BucketName = _bucketName,
                Key = fullObjectKey
            };

            var response = await _s3Client.GetObjectAsync(request, cancellationToken).ConfigureAwait(false);

            return response.ResponseStream;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to open read stream for object {ObjectKey} in container {Container}", objectKey, _bucketName);
            throw;
        }
    }

    public async Task<bool> ExistsAsync(
        string objectKey,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var fullObjectKey = GetFullObjectKey(objectKey);
            var request = new GetObjectMetadataRequest
            {
                BucketName = _bucketName,
                Key = fullObjectKey
            };

            await _s3Client.GetObjectMetadataAsync(request, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to check existence of object {ObjectKey} in container {Container}", objectKey, _bucketName);
            throw;
        }
    }

    public void Dispose()
    {
        if (_shouldDisposeClient)
        {
            _s3Client?.Dispose();
        }
    }

    /// <summary>
    /// Normalizes the top-level folder to ensure consistent format:
    /// - Removes leading and trailing slashes
    /// - Adds a single trailing slash
    /// - Handles edge cases like empty strings, whitespace, and multiple slashes
    /// </summary>
    private static string? NormalizeTopLevelFolder(string? topLevelFolder)
    {
        if (string.IsNullOrWhiteSpace(topLevelFolder))
            return null;

        // Remove all leading and trailing slashes, then add exactly one trailing slash
        var normalized = topLevelFolder.Trim().Trim('/');

        return string.IsNullOrEmpty(normalized) ? null : normalized + "/";
    }

    public override string ToString() => $"BlobStorageService(bucket={_bucketName},tlf={_topLevelFolder})";
}