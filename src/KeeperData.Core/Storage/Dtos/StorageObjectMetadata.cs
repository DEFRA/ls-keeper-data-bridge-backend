namespace KeeperData.Core.Storage.Dtos;

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