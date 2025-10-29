using Amazon.S3;
using Amazon.S3.Model;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;

namespace KeeperData.Infrastructure.Storage;

public class S3BlobStorageService : S3BlobStorageServiceReadOnly, IBlobStorageService
{
    public S3BlobStorageService(
        IAmazonS3 s3Client,
        ILogger<S3BlobStorageService> logger,
        string container,
        string? topLevelFolder = null)
        : base(s3Client, logger, container, topLevelFolder)
    {
    }

    public S3BlobStorageService(
        AmazonS3Config config,
        ILogger<S3BlobStorageService> logger,
        string container,
        string? topLevelFolder = null)
        : base(config, logger, container, topLevelFolder)
    {
    }

    public S3BlobStorageService(
        string accessKey,
        string secretKey,
        AmazonS3Config config,
        ILogger<S3BlobStorageService> logger,
        string container,
        string? topLevelFolder = null)
        : base(accessKey, secretKey, config, logger, container, topLevelFolder)
    {
    }

    public S3BlobStorageService(
        string accessKey,
        string secretKey,
        string sessionToken,
        AmazonS3Config config,
        ILogger<S3BlobStorageService> logger,
        string container,
        string? topLevelFolder = null)
        : base(accessKey, secretKey, sessionToken, config, logger, container, topLevelFolder)
    {
    }

    public S3BlobStorageService(
        string serviceUrl,
        string accessKey,
        string secretKey,
        ILogger<S3BlobStorageService> logger,
        string container,
        string? topLevelFolder = null)
        : base(serviceUrl, accessKey, secretKey, logger, container, topLevelFolder)
    {
    }

    public S3BlobStorageService(
        Amazon.RegionEndpoint region,
        ILogger<S3BlobStorageService> logger,
        string container,
        string? topLevelFolder = null)
        : base(region, logger, container, topLevelFolder)
    {
    }

    public async Task UploadAsync(
        string objectKey,
        byte[] content,
        string? contentType = null,
        IReadOnlyDictionary<string, string>? metadata = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var fullObjectKey = GetFullObjectKey(objectKey);

            using var stream = new MemoryStream(content);
            var request = new PutObjectRequest
            {
                BucketName = _bucketName,
                Key = fullObjectKey,
                InputStream = stream,
                ContentType = contentType ?? "application/octet-stream"
            };

            // Add user metadata
            if (metadata != null)
            {
                foreach (var kvp in metadata)
                {
                    request.Metadata.Add(kvp.Key, kvp.Value);
                }
            }

            await _s3Client.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);

            _logger.LogDebug("Successfully uploaded object {ObjectKey} to container {Container}", objectKey, _bucketName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to upload object {ObjectKey} to container {Container}", objectKey, _bucketName);
            throw;
        }
    }

    public async Task<Stream> OpenWriteAsync(
        string objectKey,
        string? contentType = null,
        IReadOnlyDictionary<string, string>? metadata = null,
        int partSizeBytes = 8 * 1024 * 1024,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var fullObjectKey = GetFullObjectKey(objectKey);

            // Create a multipart upload stream
            var multipartStream = new MultipartUploadStream(
                _s3Client,
                _bucketName,
                fullObjectKey,
                contentType ?? "application/octet-stream",
                metadata,
                partSizeBytes,
                _logger,
                cancellationToken);

            await multipartStream.InitializeAsync().ConfigureAwait(false);

            _logger.LogDebug("Opened write stream for object {ObjectKey} in container {Container}", objectKey, _bucketName);

            return multipartStream;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to open write stream for object {ObjectKey} in container {Container}", objectKey, _bucketName);
            throw;
        }
    }

    public async Task SetMetadataAsync(
        string objectKey,
        IReadOnlyDictionary<string, string> metadata,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var fullObjectKey = GetFullObjectKey(objectKey);

            // First get the existing object to preserve its content
            var copyRequest = new CopyObjectRequest
            {
                SourceBucket = _bucketName,
                SourceKey = fullObjectKey,
                DestinationBucket = _bucketName,
                DestinationKey = fullObjectKey,
                MetadataDirective = S3MetadataDirective.REPLACE
            };

            // Add user metadata
            foreach (var kvp in metadata)
            {
                copyRequest.Metadata.Add(kvp.Key, kvp.Value);
            }

            await _s3Client.CopyObjectAsync(copyRequest, cancellationToken).ConfigureAwait(false);

            _logger.LogDebug("Successfully updated metadata for object {ObjectKey} in container {Container}", objectKey, _bucketName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to update metadata for object {ObjectKey} in container {Container}", objectKey, _bucketName);
            throw;
        }
    }

    public async Task DeleteAsync(
        string objectKey,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var fullObjectKey = GetFullObjectKey(objectKey);
            var request = new DeleteObjectRequest
            {
                BucketName = _bucketName,
                Key = fullObjectKey
            };

            await _s3Client.DeleteObjectAsync(request, cancellationToken).ConfigureAwait(false);

            _logger.LogDebug("Successfully deleted object {ObjectKey} from container {Container}", objectKey, _bucketName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete object {ObjectKey} from container {Container}", objectKey, _bucketName);
            throw;
        }
    }
}

/// <summary>
/// Stream implementation that supports multipart uploads to S3 for large files
/// </summary>
internal class MultipartUploadStream : Stream
{
    private readonly IAmazonS3 _s3Client;
    private readonly string _bucketName;
    private readonly string _key;
    private readonly string _contentType;
    private readonly IReadOnlyDictionary<string, string>? _metadata;
    private readonly int _partSizeBytes;
    private readonly ILogger _logger;
    private readonly CancellationToken _cancellationToken;

    private string? _uploadId;
    private readonly List<PartETag> _partETags = new();
    private MemoryStream _currentPart = new();
    private int _partNumber = 1;
    private bool _disposed = false;
    private bool _finalized = false;

    public MultipartUploadStream(
        IAmazonS3 s3Client,
        string bucketName,
        string key,
        string contentType,
        IReadOnlyDictionary<string, string>? metadata,
        int partSizeBytes,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        _s3Client = s3Client;
        _bucketName = bucketName;
        _key = key;
        _contentType = contentType;
        _metadata = metadata;
        _partSizeBytes = Math.Max(partSizeBytes, 5 * 1024 * 1024); // Minimum 5MB per S3 requirements
        _logger = logger;
        _cancellationToken = cancellationToken;
    }

    public async Task InitializeAsync()
    {
        var request = new InitiateMultipartUploadRequest
        {
            BucketName = _bucketName,
            Key = _key,
            ContentType = _contentType
        };

        // Add user metadata
        if (_metadata != null)
        {
            foreach (var kvp in _metadata)
            {
                request.Metadata.Add(kvp.Key, kvp.Value);
            }
        }

        var response = await _s3Client.InitiateMultipartUploadAsync(request, _cancellationToken).ConfigureAwait(false);
        _uploadId = response.UploadId;

        _logger.LogDebug("Initiated multipart upload {UploadId} for {Key}", _uploadId, _key);
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => !_disposed && !_finalized;
    public override long Length => throw new NotSupportedException();
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Flush()
    {
        // No-op for async stream
    }

    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        if (_currentPart.Length > 0)
        {
            await UploadCurrentPartAsync().ConfigureAwait(false);
        }
    }

    public override int Read(byte[] buffer, int offset, int count)
        => throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin)
        => throw new NotSupportedException();

    public override void SetLength(long value)
        => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count)
    {
        WriteAsync(buffer, offset, count, _cancellationToken).ConfigureAwait(false).GetAwaiter().GetResult();
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        if (_disposed || _finalized)
            throw new ObjectDisposedException(nameof(MultipartUploadStream));

        var bytesToWrite = count;
        var sourceOffset = offset;

        while (bytesToWrite > 0)
        {
            var spaceInCurrentPart = _partSizeBytes - (int)_currentPart.Length;
            var bytesToWriteToPart = Math.Min(bytesToWrite, spaceInCurrentPart);

            _currentPart.Write(buffer, sourceOffset, bytesToWriteToPart);

            sourceOffset += bytesToWriteToPart;
            bytesToWrite -= bytesToWriteToPart;

            // If current part is full, upload it
            if (_currentPart.Length >= _partSizeBytes)
            {
                await UploadCurrentPartAsync().ConfigureAwait(false);
            }
        }
    }

    private async Task UploadCurrentPartAsync()
    {
        if (_currentPart.Length == 0 || _uploadId == null)
            return;

        var uploadRequest = new UploadPartRequest
        {
            BucketName = _bucketName,
            Key = _key,
            UploadId = _uploadId,
            PartNumber = _partNumber,
            InputStream = new MemoryStream(_currentPart.ToArray()),
            PartSize = _currentPart.Length
        };

        var response = await _s3Client.UploadPartAsync(uploadRequest, _cancellationToken).ConfigureAwait(false);

        _partETags.Add(new PartETag
        {
            PartNumber = _partNumber,
            ETag = response.ETag
        });

        _logger.LogDebug("Uploaded part {PartNumber} for {Key}, ETag: {ETag}", _partNumber, _key, response.ETag);

        _partNumber++;
        _currentPart.Dispose();
        _currentPart = new MemoryStream();
    }

    protected override void Dispose(bool disposing)
    {
        if (!_disposed && disposing)
        {
            // Multipart uploads are inherently async operations.
            // Synchronous disposal cannot properly finalize uploads.
            if (!_finalized)
            {
                _logger.LogWarning(
                    "MultipartUploadStream for {Key} disposed synchronously without finalization. " +
                    "Use DisposeAsync() or 'await using' to ensure proper upload completion. " +
                    "Upload will be aborted.", _key);

                // Best effort abort without blocking
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await AbortUploadAsync().ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to abort multipart upload for {Key} during synchronous disposal", _key);
                    }
                });
            }

            _currentPart?.Dispose();
            _disposed = true;
        }

        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            try
            {
                if (!_finalized)
                {
                    await FinalizeUploadAsync().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error finalizing multipart upload for {Key}", _key);

                // Attempt to abort the upload
                try
                {
                    await AbortUploadAsync().ConfigureAwait(false);
                }
                catch (Exception abortEx)
                {
                    _logger.LogError(abortEx, "Error aborting multipart upload for {Key}", _key);
                }
            }
            finally
            {
                _currentPart?.Dispose();
                _disposed = true;
            }
        }

        await base.DisposeAsync().ConfigureAwait(false);
    }

    private async Task FinalizeUploadAsync()
    {
        if (_finalized || _uploadId == null)
            return;

        try
        {
            // Upload any remaining data in the current part
            if (_currentPart.Length > 0)
            {
                await UploadCurrentPartAsync().ConfigureAwait(false);
            }

            // Complete the multipart upload
            var completeRequest = new CompleteMultipartUploadRequest
            {
                BucketName = _bucketName,
                Key = _key,
                UploadId = _uploadId,
                PartETags = _partETags
            };

            await _s3Client.CompleteMultipartUploadAsync(completeRequest, _cancellationToken).ConfigureAwait(false);

            _logger.LogDebug("Completed multipart upload {UploadId} for {Key} with {PartCount} parts",
                _uploadId, _key, _partETags.Count);

            _finalized = true;
        }
        catch (Exception)
        {
            await AbortUploadAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task AbortUploadAsync()
    {
        if (_uploadId == null)
            return;

        try
        {
            var abortRequest = new AbortMultipartUploadRequest
            {
                BucketName = _bucketName,
                Key = _key,
                UploadId = _uploadId
            };

            await _s3Client.AbortMultipartUploadAsync(abortRequest, _cancellationToken).ConfigureAwait(false);

            _logger.LogDebug("Aborted multipart upload {UploadId} for {Key}", _uploadId, _key);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to abort multipart upload {UploadId} for {Key}", _uploadId, _key);
        }
    }
}