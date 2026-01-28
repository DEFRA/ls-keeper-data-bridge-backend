using System.Diagnostics.CodeAnalysis;
using MongoDB.Bson;

namespace KeeperData.Core.Reporting.Dtos;

/// <summary>
/// Represents a lineage event for a record, tracking changes and their origin.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "DTO record - no logic to test.")]
public record RecordLineageEvent
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
    /// Gets the type of event (Created, Updated, Deleted, or Undeleted).
    /// </summary>
    public required RecordEventType EventType { get; init; }

    /// <summary>
    /// Gets the import ID that triggered this event.
    /// </summary>
    public required Guid ImportId { get; init; }

    /// <summary>
    /// Gets the file key from which this event originated.
    /// </summary>
    public required string FileKey { get; init; }

    /// <summary>
    /// Gets the UTC timestamp when this event occurred.
    /// </summary>
    public DateTime EventDateUtc { get; init; }

    /// <summary>
    /// Gets the type of change (e.g., "Insert", "Update", "Delete").
    /// </summary>
    public required string ChangeType { get; init; }

    /// <summary>
    /// Gets the previous values of the record before this change, or null if not applicable.
    /// </summary>
    public BsonDocument? PreviousValues { get; init; }

    /// <summary>
    /// Gets the new values of the record after this change, or null if not applicable.
    /// </summary>
    public BsonDocument? NewValues { get; init; }
}