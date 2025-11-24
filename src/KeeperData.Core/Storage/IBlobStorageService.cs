namespace KeeperData.Core.Storage;

public interface IBlobStorageService : IBlobStorageServiceReadOnly
{
    Task UploadAsync(
        string objectKey,
        byte[] content,
        string? contentType = null,
        IReadOnlyDictionary<string, string>? metadata = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens a writable stream that uploads to the destination as you write (suited to large or unknown-length data).
    /// Disposing the stream MUST finalize the upload (or abort on error).
    /// </summary>
    Task<Stream> OpenWriteAsync(
        string objectKey,
        string? contentType = null,
        IReadOnlyDictionary<string, string>? metadata = null,
        int partSizeBytes = 8 * 1024 * 1024,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces user metadata on an existing object (provider rules apply).
    /// </summary>
    Task SetMetadataAsync(
        string objectKey,
        IReadOnlyDictionary<string, string> metadata,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes the object if it exists (idempotent).
    /// </summary>
    Task DeleteAsync(
        string objectKey,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes all objects under the configured top-level folder prefix.
    /// Returns the list of deleted keys and the total count.
    /// </summary>
    Task<ClearDownResult> ClearDownAsync(CancellationToken cancellationToken = default);
}