using KeeperData.Core.Attributes;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace KeeperData.Core.Reporting.Domain;

[CollectionName("record_lineage")]
public class RecordLineageDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string? Id { get; init; }
    
    public required string RecordId { get; init; }
    public required string CollectionName { get; init; }
    public required List<LineageEventDocument> Events { get; init; }
    public required string CurrentStatus { get; init; }
    
    [BsonRepresentation(BsonType.String)]
    public required Guid CreatedByImport { get; init; }
    
    [BsonRepresentation(BsonType.String)]
    public required Guid LastModifiedByImport { get; init; }
    
    public DateTime CreatedAtUtc { get; init; }
    public DateTime LastModifiedAtUtc { get; init; }
}

public class LineageEventDocument
{
    public required string EventType { get; init; }
    
    [BsonRepresentation(BsonType.String)]
    public required Guid ImportId { get; init; }
    
    public required string FileKey { get; init; }
    public DateTime EventDateUtc { get; init; }
    public required string ChangeType { get; init; }
    public BsonDocument? PreviousValues { get; init; }
    public BsonDocument? NewValues { get; init; }
}
