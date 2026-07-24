using KeeperData.Core.Domain.Entities;
using KeeperData.Core.Storage.KeyRotation;

namespace KeeperData.Infrastructure.Storage.KeyRotation;

/// <summary>Details of validated credentials to activate as a new rotation.</summary>
public sealed record ValidatedRotation(
    string BucketName,
    KeyRotationSource Source,
    string AccessKeyId,
    string SecretAccessKey,
    string? FileKey = null,
    string? FileHash = null,
    string? RolledBackFromId = null);

/// <summary>Details of a rotation file that was rejected (parse or validation failure).</summary>
public sealed record FailedRotation(
    string BucketName,
    string? FileKey,
    string? FileHash,
    string KeyIdMasked,
    string? FailureReason);

/// <summary>
/// The encrypted rotation store: owns the <see cref="KeyRotationRecord"/> lifecycle —
/// building records (encryption, masking, timestamps), appending them to the history,
/// invalidating the in-process credential cache on activation, and decrypting stored
/// credentials for rollback.
/// </summary>
public interface IKeyRotationStore
{
    /// <summary>Whether the at-rest encryption key is configured. When false the feature is dormant.</summary>
    bool IsEncryptionConfigured { get; }

    /// <summary>Gets a rotation record by id, or null when not found.</summary>
    Task<KeyRotationRecord?> GetByIdAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the file hash of the most recently observed rotation file (successful or failed),
    /// or null when no file has ever been processed.
    /// </summary>
    Task<string?> GetLatestObservedFileHashAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Encrypts and activates validated credentials as a new rotation record, superseding the
    /// current active record and invalidating the in-process credential cache.
    /// </summary>
    Task<KeyRotationRecord> ActivateValidatedAsync(ValidatedRotation rotation, CancellationToken cancellationToken = default);

    /// <summary>Appends a failed rotation record (no key material is stored).</summary>
    Task RecordFailedAsync(FailedRotation failure, CancellationToken cancellationToken = default);

    /// <summary>Decrypts the credentials captured in a rotation record.</summary>
    (string AccessKeyId, string SecretAccessKey) DecryptCredentials(KeyRotationRecord record);
}
