namespace KeeperData.Core.Storage.Dtos;

public sealed record StorageListPage
{
    public required IReadOnlyList<StorageObjectInfo> Items { get; init; }
    public string? ContinuationToken { get; init; }
    public bool? IsTruncated { get; init; }
}