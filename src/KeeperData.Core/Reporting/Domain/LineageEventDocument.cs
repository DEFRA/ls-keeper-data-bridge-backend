using KeeperData.Core.Attributes;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace KeeperData.Core.Reporting.Domain;

/// <summary>
/// Represents a single lineage event in a record's history.
/// Stored in separate collection for unbounded growth.
/// ID format ensures chronological ordering: {CollectionName}__{RecordId}__{yyyyMMddHHmmssffffff}__{NNNNNN}
/// </summary>
[CollectionName("record_lineage_events")]
public class LineageEventDocument
{
    /// <summary>
    /// Composite key with chronological sorting built-in.
    /// Format: {CollectionName}__{RecordId}__{yyyyMMddHHmmssffffff}__{NNNNNN}
    /// Example: "sam_cph_holdings__CPH001__20241215143055123456__042891"
    /// MongoDB will naturally sort these chronologically.
    /// </summary>
    [BsonId]
    public required string Id { get; init; }

    /// <summary>
    /// Foreign key to RecordLineageDocument: {CollectionName}__{RecordId}
    /// Example: "sam_cph_holdings__CPH001"
    /// Used for efficient querying of all events for a record.
    /// </summary>
    public required string LineageDocumentId { get; init; }

    /// <summary>
    /// Denormalized: The business key of the record (e.g., "CPH001")
    /// Improves query flexibility without joins.
    /// </summary>
    public required string RecordId { get; init; }

    /// <summary>
    /// Denormalized: The MongoDB collection name (e.g., "sam_cph_holdings")
    /// Enables efficient collection-level queries.
    /// </summary>
    public required string CollectionName { get; init; }

    /// <summary>
    /// Event type: "Created", "Updated", "Deleted", "Undeleted"
    /// </summary>
    public required string EventType { get; init; }

    /// <summary>
    /// Import that caused this event
    /// </summary>
    [BsonRepresentation(BsonType.String)]
    public required Guid ImportId { get; init; }

    /// <summary>
    /// Source file key that triggered this event
    /// </summary>
    public required string FileKey { get; init; }

    /// <summary>
    /// When this event occurred
    /// </summary>
    public DateTime EventDateUtc { get; init; }

    /// <summary>
    /// Change type from CSV: "I", "U", "D"
    /// </summary>
    public required string ChangeType { get; init; }

    /// <summary>
    /// Full snapshot of record before this change (null for creates)
    /// </summary>
    public BsonDocument? PreviousValues { get; init; }

    /// <summary>
    /// Full snapshot of record after this change (null for deletes)
    /// </summary>
    public BsonDocument? NewValues { get; init; }
}
