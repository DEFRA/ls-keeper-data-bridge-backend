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
}


/// <summary>
/// Lightweight listing descriptor for discovery.
/// </summary>
public sealed record StorageObjectInfo
{
    public required string Container { get; init; }
    public required string Key { get; init; }
    public long Size { get; init; }
    public DateTimeOffset LastModified { get; init; }
    public string? ETag { get; init; }
    public required Uri StorageUri { get; init; }
    public Uri? HttpUri { get; init; }
}


public sealed record StorageListPage
{
    public required IReadOnlyList<StorageObjectInfo> Items { get; init; }
    public string? ContinuationToken { get; init; }
    public bool? IsTruncated { get; init; }
}

/// <summary>
/// Rich metadata model for a single object.
/// </summary>
public sealed record StorageObjectMetadata
{
    public required string Container { get; init; }
    public required string Key { get; init; }
    public long ContentLength { get; init; }
    public string? ContentType { get; init; }
    public string? ETag { get; init; }
    public DateTimeOffset? LastModified { get; init; }
    public string? StorageClass { get; init; }
    public string? Encryption { get; init; }
    public required Uri StorageUri { get; init; }
    public Uri? HttpUri { get; init; }
    public required IReadOnlyDictionary<string, string> UserMetadata { get; init; }
    public IReadOnlyDictionary<string, string>? ProviderProperties { get; init; } = null;
}