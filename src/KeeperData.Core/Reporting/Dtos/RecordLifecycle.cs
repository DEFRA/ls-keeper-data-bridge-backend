namespace KeeperData.Core.Reporting.Dtos;

/// <summary>
/// Represents the complete lifecycle of a record, including all changes and events.
/// </summary>
public record RecordLifecycle
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
    /// Gets the import ID that originally created this record.
    /// </summary>
    public required Guid CreatedByImport { get; init; }

    /// <summary>
    /// Gets the import ID that last modified this record.
    /// </summary>
    public required Guid LastModifiedByImport { get; init; }

    /// <summary>
    /// Gets the UTC timestamp when the record was created.
    /// </summary>
    public DateTime CreatedAtUtc { get; init; }

    /// <summary>
    /// Gets the UTC timestamp when the record was last modified.
    /// </summary>
    public DateTime LastModifiedAtUtc { get; init; }

    /// <summary>
    /// Gets the list of all events in the record's lifecycle.
    /// </summary>
    public required IReadOnlyList<RecordLineageEvent> Events { get; init; }
}