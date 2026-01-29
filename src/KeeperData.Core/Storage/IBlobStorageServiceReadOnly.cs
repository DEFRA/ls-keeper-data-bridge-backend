using KeeperData.Core.Storage.Dtos;

namespace KeeperData.Core.Storage;

/// <summary>
/// Read-only abstraction over a cloud blob/object storage system.
/// Supports discovery, metadata, full downloads, and streaming.
/// </summary>
public interface IBlobStorageServiceReadOnly
{
    /// <summary>
    /// Lists objects within the configured container optionally under a prefix/path.
    /// </summary>
    Task<IReadOnlyList<StorageObjectInfo>> ListAsync(
        string? prefix = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieve a single page of items. Set pageSize (MaxKeys) up to provider limits (S3: 1–1000).
    /// Pass continuationToken returned from a prior call to get the next page.
    /// </summary>
    Task<StorageListPage> ListPageAsync(
        string? prefix = null,
        int pageSize = 1000,
        string? continuationToken = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves object metadata (size, ETag, content type, timestamps, URIs, and user metadata).
    /// </summary>
    Task<StorageObjectMetadata> GetMetadataAsync(
        string objectKey,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Downloads an object fully into memory.
    /// </summary>
    Task<byte[]> DownloadAsync(
        string objectKey,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens a readable stream to the object (caller disposes the returned stream).
    /// </summary>
    Task<Stream> OpenReadAsync(
        string objectKey,
        CancellationToken cancellationToken = default);

    Task<bool> ExistsAsync(
        string objectKey,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Generates a presigned URL for downloading an object.
    /// </summary>
    /// <param name="objectKey">The object key.</param>
    /// <param name="expiresIn">Optional expiry duration. Defaults to 7 days if not specified.</param>
    /// <returns>A presigned URL for downloading the object.</returns>
    string GeneratePresignedUrl(string objectKey, TimeSpan? expiresIn = null);

}