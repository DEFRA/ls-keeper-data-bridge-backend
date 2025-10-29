namespace KeeperData.Core.Storage.Dtos;

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