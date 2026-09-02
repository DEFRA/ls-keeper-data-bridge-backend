using System.Collections.Concurrent;
using System.Text;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.EndToEnd.Harness;

/// <summary>
/// An <see cref="IBlobStorageService"/> backed by a dictionary. The only thing standing between the
/// end-to-end suite and S3: everything else in the pipeline runs for real.
///
/// One instance is one container, so the source bucket and each ETL folder are separate instances
/// and cannot see one another's keys, exactly as the real folders cannot.
/// </summary>
public class InMemoryBlobStorage(string container) : IBlobStorageService
{
    private readonly ConcurrentDictionary<string, StoredObject> _objects = new(StringComparer.Ordinal);

    /// <summary>Every key currently held, in ordinal order, for assertions.</summary>
    public IReadOnlyList<string> Keys => [.. _objects.Keys.OrderBy(key => key, StringComparer.Ordinal)];

    /// <summary>Seeds an object directly, bypassing the pipeline. For arranging a test.</summary>
    public void Seed(string objectKey, byte[] content)
        => _objects[objectKey] = new StoredObject(content, []);

    public string TextOf(string objectKey) => Encoding.UTF8.GetString(_objects[objectKey].Content);

    public byte[] BytesOf(string objectKey) => _objects[objectKey].Content;

    public Task<IReadOnlyList<StorageObjectInfo>> ListAsync(string? prefix = null, CancellationToken cancellationToken = default)
        => Task.FromResult<IReadOnlyList<StorageObjectInfo>>(
            [.. _objects.Keys
                .Where(key => prefix is null || key.StartsWith(prefix, StringComparison.Ordinal))
                .OrderBy(key => key, StringComparer.Ordinal)
                .Select(Info)]);

    public Task<StorageObjectMetadata> GetMetadataAsync(string objectKey, CancellationToken cancellationToken = default)
    {
        var entry = _objects[objectKey];

        return Task.FromResult(new StorageObjectMetadata
        {
            Container = container,
            Key = objectKey,
            ContentLength = entry.Content.Length,
            StorageUri = UriFor(objectKey),
            UserMetadata = entry.Metadata
        });
    }

    public Task<byte[]> DownloadAsync(string objectKey, CancellationToken cancellationToken = default)
        => Task.FromResult(_objects[objectKey].Content);

    public Task<Stream> OpenReadAsync(string objectKey, CancellationToken cancellationToken = default)
        => Task.FromResult<Stream>(new MemoryStream(_objects[objectKey].Content, writable: false));

    public Task<bool> ExistsAsync(string objectKey, CancellationToken cancellationToken = default)
        => Task.FromResult(_objects.ContainsKey(objectKey));

    public Task UploadAsync(
        string objectKey,
        byte[] content,
        string? contentType = null,
        IReadOnlyDictionary<string, string>? metadata = null,
        CancellationToken cancellationToken = default)
    {
        _objects[objectKey] = new StoredObject(content, metadata?.ToDictionary() ?? []);
        return Task.CompletedTask;
    }

    public Task<Stream> OpenWriteAsync(
        string objectKey,
        string? contentType = null,
        IReadOnlyDictionary<string, string>? metadata = null,
        int partSizeBytes = 8 * 1024 * 1024,
        CancellationToken cancellationToken = default)
        => Task.FromResult<Stream>(new CommitOnDisposeStream(
            content => _objects[objectKey] = new StoredObject(content, metadata?.ToDictionary() ?? [])));

    public Task SetMetadataAsync(string objectKey, IReadOnlyDictionary<string, string> metadata, CancellationToken cancellationToken = default)
    {
        var entry = _objects[objectKey];
        _objects[objectKey] = entry with { Metadata = metadata.ToDictionary() };
        return Task.CompletedTask;
    }

    public Task DeleteAsync(string objectKey, CancellationToken cancellationToken = default)
    {
        _objects.TryRemove(objectKey, out _);
        return Task.CompletedTask;
    }

    public Task<ClearDownResult> DeleteByPrefixAsync(string prefix, CancellationToken cancellationToken = default)
    {
        var keys = _objects.Keys.Where(key => key.StartsWith(prefix, StringComparison.Ordinal)).ToList();
        foreach (var key in keys)
            _objects.TryRemove(key, out _);

        return Task.FromResult(new ClearDownResult { DeletedKeys = keys, TotalDeleted = keys.Count });
    }

    public Task<ClearDownResult> ClearDownAsync(CancellationToken cancellationToken = default)
    {
        var keys = _objects.Keys.ToList();
        _objects.Clear();

        return Task.FromResult(new ClearDownResult { DeletedKeys = keys, TotalDeleted = keys.Count });
    }

    public Task<StorageListPage> ListPageAsync(
        string? prefix = null,
        int pageSize = 1000,
        string? continuationToken = null,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException("The ETL pipeline lists without paging; add this when it stops doing so.");

    public string GeneratePresignedUrl(string objectKey, TimeSpan? expiresIn = null) => UriFor(objectKey).ToString();

    private StorageObjectInfo Info(string objectKey) => new()
    {
        Container = container,
        Key = objectKey,
        Size = _objects[objectKey].Content.Length,
        StorageUri = UriFor(objectKey)
    };

    private Uri UriFor(string objectKey) => new($"memory://{container}/{objectKey}");

    private sealed record StoredObject(byte[] Content, Dictionary<string, string> Metadata);

    /// <summary>The pipeline writes through streams it disposes, so the object is only committed on
    /// dispose. A half-written stream therefore leaves no object behind, which is what the real
    /// storage does and what the artefact-write retry logic depends on.</summary>
    private sealed class CommitOnDisposeStream(Action<byte[]> commit) : MemoryStream
    {
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                commit(ToArray());
            }

            base.Dispose(disposing);
        }
    }
}
