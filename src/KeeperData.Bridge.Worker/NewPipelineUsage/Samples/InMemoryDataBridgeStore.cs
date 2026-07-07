using System.Collections.Concurrent;
using KeeperData.Core.Ingestion.Contracts;
using KeeperData.Core.Ingestion.Models;

namespace KeeperData.Bridge.Worker.NewPipelineUsage.Samples;

/// <summary>In-memory implementation of the durable-area facade so the demo needs no real S3/disk.
/// One <see cref="InMemoryBlobArea"/> per <see cref="BridgeArea"/>.</summary>
public sealed class InMemoryDataBridgeStore : IDataBridgeStore
{
    private readonly ConcurrentDictionary<BridgeArea, InMemoryBlobArea> _areas = new();

    public IBlobArea Area(BridgeArea area) => _areas.GetOrAdd(area, a => new InMemoryBlobArea(a));
}

/// <summary>In-memory area. Keys map to byte payloads; writes are atomic (the buffer is only
/// published once the write delegate completes), mirroring the temp-then-swap contract.</summary>
public sealed class InMemoryBlobArea(BridgeArea area) : IBlobArea
{
    private readonly ConcurrentDictionary<string, byte[]> _blobs = new();

    public StorageLocation PathFor(string key) => new(area, key);

    public Task<bool> ExistsAsync(StorageLocation location, CancellationToken cancellationToken)
        => Task.FromResult(_blobs.ContainsKey(location.Key));

    public Task<Stream> OpenReadAsync(StorageLocation location, CancellationToken cancellationToken)
        => _blobs.TryGetValue(location.Key, out var bytes)
            ? Task.FromResult<Stream>(new MemoryStream(bytes, writable: false))
            : throw new FileNotFoundException($"No blob at {location.Area}/{location.Key}");

    public async Task WriteAtomicAsync(StorageLocation location, Func<Stream, Task> write, CancellationToken cancellationToken)
    {
        using var buffer = new MemoryStream();
        await write(buffer);                              // stage streams into the buffer
        _blobs[location.Key] = buffer.ToArray();          // publish only on success (atomic swap)
    }

    public async IAsyncEnumerable<StorageLocation> ListAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var key in _blobs.Keys)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return new StorageLocation(area, key);
            await Task.CompletedTask;
        }
    }
}
