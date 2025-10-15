namespace KeeperData.Core.Reporting.Dtos;

public record RecordLifecycle
{
    public required string RecordId { get; init; }
    public required string CollectionName { get; init; }
    public required string CurrentStatus { get; init; }
    public required Guid CreatedByImport { get; init; }
    public required Guid LastModifiedByImport { get; init; }
    public DateTime CreatedAtUtc { get; init; }
    public DateTime LastModifiedAtUtc { get; init; }
    public required IReadOnlyList<RecordLineageEvent> Events { get; init; }
}
