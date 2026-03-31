namespace KeeperData.Infrastructure.Storage;

internal sealed class SidecarMetadata
{
    public string? ContentType { get; set; }
    public string? ETag { get; set; }
    public Dictionary<string, string> UserMetadata { get; set; } = new();
}
