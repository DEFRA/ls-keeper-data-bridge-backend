using KeeperData.Core.Domain.Entities;
using KeeperData.Core.Storage.KeyRotation;

namespace KeeperData.Infrastructure.Storage.KeyRotation;

/// <summary>
/// Default <see cref="IKeyRotationStore"/> implementation over the Mongo repository,
/// the AES-GCM secret protector, and the rotating credential provider's cache.
/// </summary>
public sealed class KeyRotationStore(
    IKeyRotationRepository repository,
    ISecretProtector secretProtector,
    IExternalStorageCredentialsProvider credentialsProvider,
    TimeProvider timeProvider) : IKeyRotationStore
{
    public bool IsEncryptionConfigured => secretProtector.IsConfigured;

    public Task<KeyRotationRecord?> GetByIdAsync(string id, CancellationToken cancellationToken = default) =>
        repository.GetByIdAsync(id, cancellationToken);

    public Task<string?> GetLatestObservedFileHashAsync(CancellationToken cancellationToken = default) =>
        repository.GetLatestObservedFileHashAsync(cancellationToken);

    public async Task<KeyRotationRecord> ActivateValidatedAsync(ValidatedRotation rotation, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rotation);

        var now = timeProvider.GetUtcNow().UtcDateTime;
        var record = new KeyRotationRecord
        {
            Id = Guid.NewGuid().ToString(),
            BucketName = rotation.BucketName,
            RotatedAtUtc = now,
            Source = rotation.Source,
            Status = KeyRotationStatus.Active,
            FileKey = rotation.FileKey,
            FileHash = rotation.FileHash,
            KeyIdMasked = KeyIdMask.Mask(rotation.AccessKeyId),
            EncryptedAccessKeyId = secretProtector.Protect(rotation.AccessKeyId, SecretPurposes.AccessKeyId),
            EncryptedSecretAccessKey = secretProtector.Protect(rotation.SecretAccessKey, SecretPurposes.SecretAccessKey),
            ValidatedAtUtc = now,
            RolledBackFromId = rotation.RolledBackFromId
        };

        await repository.ActivateAsync(record, cancellationToken);
        credentialsProvider.Invalidate();

        return record;
    }

    public Task RecordFailedAsync(FailedRotation failure, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(failure);

        var record = new KeyRotationRecord
        {
            Id = Guid.NewGuid().ToString(),
            BucketName = failure.BucketName,
            RotatedAtUtc = timeProvider.GetUtcNow().UtcDateTime,
            Source = KeyRotationSource.Automatic,
            Status = KeyRotationStatus.Failed,
            FileKey = failure.FileKey,
            FileHash = failure.FileHash,
            KeyIdMasked = failure.KeyIdMasked,
            FailureReason = failure.FailureReason
        };

        return repository.AddFailedAsync(record, cancellationToken);
    }

    public (string AccessKeyId, string SecretAccessKey) DecryptCredentials(KeyRotationRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);

        if (record.EncryptedAccessKeyId is null || record.EncryptedSecretAccessKey is null)
        {
            throw new InvalidOperationException($"Rotation record '{record.Id}' holds no key material.");
        }

        return (
            secretProtector.Unprotect(record.EncryptedAccessKeyId, SecretPurposes.AccessKeyId),
            secretProtector.Unprotect(record.EncryptedSecretAccessKey, SecretPurposes.SecretAccessKey));
    }
}
