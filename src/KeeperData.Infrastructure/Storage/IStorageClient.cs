namespace KeeperData.Infrastructure.Storage;

public interface IStorageClient
{
    string ClientName { get; }
}


/// <summary>
/// Lightweight listing descriptor for discovery.
/// </summary>
public sealed record StorageObjectInfo
{
    public string Container { get; init; }
    public string Key { get; init; }
    public long Size { get; init; }
    public DateTimeOffset LastModified { get; init; }
    public string? ETag { get; init; }
    public Uri StorageUri { get; init; }
    public  Uri? HttpUri { get; init; }
}


public sealed record StorageListPage
{
    public IReadOnlyList<StorageObjectInfo> Items { get; init; }
    public string? ContinuationToken { get; init; }
    public bool? IsTruncated { get; init; }
}

/// <summary>
/// Rich metadata model for a single object.
/// </summary>
public sealed record StorageObjectMetadata
{
    public string Container { get; init; }
    public string Key { get; init; }
    public long ContentLength { get; init; }
    public string? ContentType { get; init; }
    public string? ETag { get; init; }
    public DateTimeOffset? LastModified { get; init; }
    public string? StorageClass { get; init; }
    public string? Encryption { get; init; }
    public Uri StorageUri { get; init; }
    public Uri? HttpUri { get; init; }
    public IReadOnlyDictionary<string, string> UserMetadata { get; init; }
    public IReadOnlyDictionary<string, string>? ProviderProperties { get; init; } = null;
}
    