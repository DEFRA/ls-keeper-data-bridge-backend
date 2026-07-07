using KeeperData.Core.Ingestion.Models;

namespace KeeperData.Core.Ingestion.Contracts;

/// <summary>One durable area. Idempotency lives here: a stage checks Exists before writing,
/// and writes via WriteAtomicAsync (temp then swap) so a crash never leaves a half file.</summary>
public interface IBlobArea
{
    StorageLocation PathFor(string key);
    Task<bool> ExistsAsync(StorageLocation location, CancellationToken cancellationToken);
    Task<Stream> OpenReadAsync(StorageLocation location, CancellationToken cancellationToken);
    Task WriteAtomicAsync(StorageLocation location, Func<Stream, Task> write, CancellationToken cancellationToken);
    IAsyncEnumerable<StorageLocation> ListAsync(CancellationToken cancellationToken);
}
