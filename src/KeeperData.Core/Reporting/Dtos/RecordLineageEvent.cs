using MongoDB.Bson;

namespace KeeperData.Core.Reporting.Dtos;

public record RecordLineageEvent
{
    public required string RecordId { get; init; }
    public required string CollectionName { get; init; }
    public required RecordEventType EventType { get; init; }
    public required Guid ImportId { get; init; }
    public required string FileKey { get; init; }
    public DateTime EventDateUtc { get; init; }
    public required string ChangeType { get; init; }
    public BsonDocument? PreviousValues { get; init; }
    public BsonDocument? NewValues { get; init; }
}
