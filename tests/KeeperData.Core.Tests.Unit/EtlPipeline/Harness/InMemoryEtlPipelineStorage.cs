using System.Collections.Concurrent;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;

namespace KeeperData.Core.Tests.Unit.EtlPipeline.Harness;

/// <summary>In-memory <see cref="IEtlPipelineStorageProvider"/>, one folder per storage service,
/// so a stage can be exercised against the pipeline folders without S3 or the file system.</summary>
public sealed class InMemoryEtlPipelineStorage : IEtlPipelineStorageProvider
{
    private readonly ConcurrentDictionary<string, InMemoryBlobStorage> _folders = new();

    public InMemoryBlobStorage Folder(string folder) => _folders.GetOrAdd(folder, name => new InMemoryBlobStorage(name));

    public IBlobStorageService ForFolder(string folder) => Folder(folder);
}

public sealed class InMemoryBlobStorage(string container) : IBlobStorageService
{
    private readonly Dictionary<string, (byte[] Content, Dictionary<string, string> Metadata)> _objects = [];

    public IReadOnlyCollection<string> Keys => [.. _objects.Keys];

    public void Put(string objectKey, string content, IReadOnlyDictionary<string, string>? metadata = null)
        => _objects[objectKey] = (System.Text.Encoding.UTF8.GetBytes(content), metadata?.ToDictionary() ?? []);

    public void Put(string objectKey, byte[] content, IReadOnlyDictionary<string, string>? metadata = null)
        => _objects[objectKey] = (content, metadata?.ToDictionary() ?? []);

    public string ContentOf(string objectKey) => System.Text.Encoding.UTF8.GetString(_objects[objectKey].Content);

    public byte[] BytesOf(string objectKey) => _objects[objectKey].Content;

    public IReadOnlyDictionary<string, string> MetadataOf(string objectKey) => _objects[objectKey].Metadata;

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
            StorageUri = Uri(objectKey),
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
        _objects[objectKey] = (content, metadata?.ToDictionary() ?? []);
        return Task.CompletedTask;
    }

    public Task<Stream> OpenWriteAsync(
        string objectKey,
        string? contentType = null,
        IReadOnlyDictionary<string, string>? metadata = null,
        int partSizeBytes = 8 * 1024 * 1024,
        CancellationToken cancellationToken = default)
        => Task.FromResult<Stream>(new CommitOnDisposeStream(content => _objects[objectKey] = (content, metadata?.ToDictionary() ?? [])));

    public Task SetMetadataAsync(string objectKey, IReadOnlyDictionary<string, string> metadata, CancellationToken cancellationToken = default)
    {
        var entry = _objects[objectKey];
        _objects[objectKey] = (entry.Content, metadata.ToDictionary());
        return Task.CompletedTask;
    }

    public Task DeleteAsync(string objectKey, CancellationToken cancellationToken = default)
    {
        _objects.Remove(objectKey);
        return Task.CompletedTask;
    }

    public Task<ClearDownResult> DeleteByPrefixAsync(string prefix, CancellationToken cancellationToken = default)
    {
        var keys = _objects.Keys.Where(key => key.StartsWith(prefix, StringComparison.Ordinal)).ToList();
        foreach (var key in keys)
            _objects.Remove(key);

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
        => throw new NotSupportedException();

    public string GeneratePresignedUrl(string objectKey, TimeSpan? expiresIn = null) => Uri(objectKey).ToString();

    private StorageObjectInfo Info(string objectKey) => new()
    {
        Container = container,
        Key = objectKey,
        Size = _objects[objectKey].Content.Length,
        StorageUri = Uri(objectKey)
    };

    private Uri Uri(string objectKey) => new($"memory://{container}/{objectKey}");

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
