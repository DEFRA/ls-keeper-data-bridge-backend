using KeeperData.Core.Attributes;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace KeeperData.Core.Reporting.Domain;

/// <summary>
/// Represents the header/metadata for a record's lineage tracking.
/// One document per unique record across all imports.
/// </summary>
[CollectionName("record_lineage")]
public class RecordLineageDocument
{
    /// <summary>
    /// Composite key: {CollectionName}__{RecordId}
    /// Example: "sam_cph_holdings__CPH001"
    /// Enables direct O(1) lookup without scanning.
    /// </summary>
    [BsonId]
    public required string Id { get; init; }

    /// <summary>
    /// The business key of the record (e.g., "CPH001")
    /// </summary>
    public required string RecordId { get; init; }

    /// <summary>
    /// The MongoDB collection name (e.g., "sam_cph_holdings")
    /// </summary>
    public required string CollectionName { get; init; }

    /// <summary>
    /// Current status: "Active" or "Deleted"
    /// </summary>
    public required string CurrentStatus { get; init; }

    /// <summary>
    /// Import that originally created this record
    /// </summary>
    [BsonRepresentation(BsonType.String)]
    public required Guid CreatedByImport { get; init; }

    /// <summary>
    /// Import that most recently modified this record
    /// </summary>
    [BsonRepresentation(BsonType.String)]
    public required Guid LastModifiedByImport { get; init; }

    /// <summary>
    /// When the record was first created
    /// </summary>
    public DateTime CreatedAtUtc { get; init; }

    /// <summary>
    /// When the record was last modified
    /// </summary>
    public DateTime LastModifiedAtUtc { get; init; }
}