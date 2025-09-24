namespace KeeperData.Infrastructure.Storage;

public interface IReadOnlyStorageClient : IStorageClient
{
    // <summary>
    /// Lists objects within a logical container (e.g., bucket) optionally under a prefix/path.
    /// </summary>
    Task<IReadOnlyList<StorageObjectInfo>> ListAsync(
        string container,
        string? prefix = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieve a single page of items. Set pageSize (MaxKeys) up to provider limits (S3: 1–1000).
    /// Pass continuationToken returned from a prior call to get the next page.
    /// </summary>
    Task<StorageListPage> ListPageAsync(
        string container,
        string? prefix = null,
        int pageSize = 1000,
        string? continuationToken = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves object metadata (size, ETag, content type, timestamps, URIs, and user metadata).
    /// </summary>
    Task<StorageObjectMetadata> GetMetadataAsync(
        string container,
        string objectKey,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Downloads an object fully into memory.
    /// </summary>
    Task<byte[]> DownloadAsync(
        string container,
        string objectKey,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens a readable stream to the object (caller disposes the returned stream).
    /// </summary>
    Task<Stream> OpenReadAsync(
        string container,
        string objectKey,
        CancellationToken cancellationToken = default);

    Task<bool> ExistsAsync(
        string container, string objectKey, CancellationToken cancellationToken = default);
}