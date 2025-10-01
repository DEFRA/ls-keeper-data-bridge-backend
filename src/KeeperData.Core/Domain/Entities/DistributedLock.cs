using KeeperData.Core.Attributes;
using MongoDB.Bson.Serialization.Attributes;

namespace KeeperData.Core.Domain.Entities;

[CollectionName("distributed_locks")]
public class DistributedLock : IEntity
{
    [BsonId]
    public required string Id { get; set; }
    public string Owner { get; set; } = string.Empty;
    public DateTimeOffset ExpiresAtUtc { get; set; }
}