using KeeperData.Core.Domain.Entities;

namespace KeeperData.Application.Queries.KeyRotations;

/// <summary>
/// A display-safe key rotation summary. Never exposes key material beyond the masked hint.
/// </summary>
public record KeyRotationListItem
{
    public required string Id { get; init; }
    public required DateTime RotatedAtUtc { get; init; }
    public string? FileKey { get; init; }
    public string? FileHash { get; init; }
    public required string KeyIdHint { get; init; }
    public required string Source { get; init; }
    public required string Status { get; init; }
    public string? RolledBackFromId { get; init; }

    public static KeyRotationListItem FromRecord(KeyRotationRecord record) => new()
    {
        Id = record.Id,
        RotatedAtUtc = record.RotatedAtUtc,
        FileKey = record.FileKey,
        FileHash = record.FileHash,
        KeyIdHint = record.KeyIdMasked,
        Source = record.Source.ToString(),
        Status = record.Status.ToString(),
        RolledBackFromId = record.RolledBackFromId
    };
}

/// <summary>A page of key rotations, most recent first.</summary>
public record KeyRotationListResult
{
    public required IReadOnlyList<KeyRotationListItem> Items { get; init; }
    public required int Page { get; init; }
    public required int PageSize { get; init; }
    public required long TotalCount { get; init; }
}
