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
    /// Deletes every object whose key starts with <paramref name="prefix"/> within this service's
    /// configured top-level folder. An empty prefix deletes every object in that folder.
    /// </summary>
    /// <remarks>
    /// Returned keys are relative to the configured top-level folder, consistently with
    /// <see cref="IBlobStorageServiceReadOnly.ListAsync"/>.
    /// </remarks>
    Task<ClearDownResult> DeleteByPrefixAsync(
        string prefix,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes all objects under the configured top-level folder prefix.
    /// Returns the list of deleted keys and the total count.
    /// </summary>
    Task<ClearDownResult> ClearDownAsync(CancellationToken cancellationToken = default);
}
