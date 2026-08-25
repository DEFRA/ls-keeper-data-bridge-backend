using KeeperData.Core.Attributes;
using KeeperData.Core.Storage.KeyRotation;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace KeeperData.Core.Domain.Entities;

/// <summary>
/// A single external storage (S3) access key rotation event.
/// Records are append-only: activating a new key supersedes the previous
/// <see cref="KeyRotationStatus.Active"/> record rather than mutating history.
/// </summary>
[CollectionName("external_storage_key_rotations")]
public class KeyRotationRecord : IEntity
{
    [BsonId]
    public required string Id { get; set; }

    /// <summary>The external bucket these credentials were validated against.</summary>
    public required string BucketName { get; set; }

    /// <summary>When the rotation event occurred (UTC).</summary>
    public DateTime RotatedAtUtc { get; set; }

    [BsonRepresentation(BsonType.String)]
    public KeyRotationSource Source { get; set; }

    [BsonRepresentation(BsonType.String)]
    public KeyRotationStatus Status { get; set; }

    /// <summary>The S3 object key of the rotation file. Null for manual/rollback rotations.</summary>
    public string? FileKey { get; set; }

    /// <summary>Lowercase hex SHA-256 of the rotation file content. Null for manual/rollback rotations.</summary>
    public string? FileHash { get; set; }

    /// <summary>Display-safe masked access key id (first three and last three characters).</summary>
    public required string KeyIdMasked { get; set; }

    /// <summary>Encrypted access key id. Null for <see cref="KeyRotationStatus.Failed"/> records.</summary>
    public EncryptedSecret? EncryptedAccessKeyId { get; set; }

    /// <summary>Encrypted secret access key. Null for <see cref="KeyRotationStatus.Failed"/> records.</summary>
    public EncryptedSecret? EncryptedSecretAccessKey { get; set; }

    /// <summary>When the credentials were successfully validated against the bucket (UTC). Null for failed records.</summary>
    public DateTime? ValidatedAtUtc { get; set; }

    /// <summary>For rollback records, the id of the rotation record the key was restored from.</summary>
    public string? RolledBackFromId { get; set; }

    /// <summary>For failed records, why the rotation was rejected. Never contains key material.</summary>
    public string? FailureReason { get; set; }
}
