namespace KeeperData.Core.Reporting.Dtos;

/// <summary>
/// Represents a paginated list of lineage events for a record.
/// </summary>
public record PaginatedLineageEvents
{
    /// <summary>
    /// Gets the unique identifier of the record.
    /// </summary>
    public required string RecordId { get; init; }

    /// <summary>
    /// Gets the name of the collection containing the record.
    /// </summary>
    public required string CollectionName { get; init; }

    /// <summary>
    /// Gets the current status of the record (e.g., "Active", "Deleted").
    /// </summary>
    public required string CurrentStatus { get; init; }

    /// <summary>
    /// Gets the total number of events for this record.
    /// </summary>
    public int TotalEvents { get; init; }

    /// <summary>
    /// Gets the number of events to skip.
    /// </summary>
    public int Skip { get; init; }

    /// <summary>
    /// Gets the maximum number of events to return.
    /// </summary>
    public int Top { get; init; }

    /// <summary>
    /// Gets the actual number of events returned in this page.
    /// </summary>
    public int Count { get; init; }

    /// <summary>
    /// Gets the list of lineage events for this page.
    /// </summary>
    public required IReadOnlyList<RecordLineageEvent> Events { get; init; }

    /// <summary>
    /// Gets the UTC timestamp when the record was created.
    /// </summary>
    public DateTime CreatedAtUtc { get; init; }

    /// <summary>
    /// Gets the UTC timestamp when the record was last modified.
    /// </summary>
    public DateTime LastModifiedAtUtc { get; init; }

    /// <summary>
    /// Gets the import ID that created this record.
    /// </summary>
    public Guid CreatedByImport { get; init; }

    /// <summary>
    /// Gets the import ID that last modified this record.
    /// </summary>
    public Guid LastModifiedByImport { get; init; }
}