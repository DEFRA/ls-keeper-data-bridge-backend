using Amazon.S3;
using Amazon.S3.Model;
using KeeperData.Core.Domain.Entities;
using KeeperData.Core.Locking;
using KeeperData.Core.Storage.KeyRotation;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Factories;
using KeeperData.Infrastructure.Storage.KeyRotation.Configuration;
using Microsoft.Extensions.Logging;
using System.Net;
using System.Security.Cryptography;

namespace KeeperData.Infrastructure.Storage.KeyRotation;

/// <summary>
/// Orchestrates external storage access-key rotation: the daily file check, manual
/// application, and rollback. All state changes are serialised behind a distributed lock
/// and recorded append-only via <see cref="IKeyRotationRepository"/>.
/// </summary>
public sealed class KeyRotationService(
    IS3ClientFactory s3ClientFactory,
    IKeyRotationRepository repository,
    ISecretProtector secretProtector,
    IS3CredentialValidator credentialValidator,
    IDistributedLock distributedLock,
    IExternalStorageCredentialsProvider credentialsProvider,
    ExternalStorageKeyRotationOptions options,
    TimeProvider timeProvider,
    ILogger<KeyRotationService> logger) : IKeyRotationService
{
    private const string LogPrefix = "[KeyRotation]";

    public async Task<KeyRotationCheckResult> CheckAndRotateAsync(CancellationToken cancellationToken = default)
    {
        var bucketName = s3ClientFactory.GetClientBucketName<ExternalStorageClient>();

        if (!secretProtector.IsConfigured)
        {
            return new KeyRotationCheckResult(
                KeyRotationCheckOutcome.NotConfigured,
                bucketName,
                Detail: $"Encryption key '{options.EncryptionKeySecretName}' is not configured");
        }

        var fileKey = KeyRotationFileNameResolver.Resolve(bucketName);

        await using var lockHandle = await distributedLock.TryAcquireAsync(
            options.LockName, TimeSpan.FromMinutes(options.LockDurationMinutes), cancellationToken);

        if (lockHandle is null)
        {
            return new KeyRotationCheckResult(KeyRotationCheckOutcome.LockUnavailable, bucketName, fileKey);
        }

        byte[] fileContent;
        try
        {
            var downloaded = await TryDownloadFileAsync(bucketName, fileKey, cancellationToken);
            if (downloaded is null)
            {
                return new KeyRotationCheckResult(KeyRotationCheckOutcome.FileNotFound, bucketName, fileKey);
            }

            fileContent = downloaded;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "{LogPrefix} Could not read rotation file {FileKey} from bucket {BucketName}",
                LogPrefix, fileKey, bucketName);

            return new KeyRotationCheckResult(
                KeyRotationCheckOutcome.TransientError, bucketName, fileKey,
                Detail: $"Could not read the rotation file: {ex.GetType().Name}");
        }

        var fileHash = Convert.ToHexString(SHA256.HashData(fileContent)).ToLowerInvariant();

        var lastObservedHash = await repository.GetLatestObservedFileHashAsync(cancellationToken);
        if (string.Equals(fileHash, lastObservedHash, StringComparison.Ordinal))
        {
            return new KeyRotationCheckResult(KeyRotationCheckOutcome.AlreadyProcessed, bucketName, fileKey, fileHash);
        }

        AccessKeyCsvContent parsed;
        try
        {
            using var contentStream = new MemoryStream(fileContent, writable: false);
            parsed = AccessKeyCsvParser.Parse(contentStream);
        }
        catch (AccessKeyFileFormatException ex)
        {
            await repository.AddFailedAsync(CreateRecord(
                bucketName, KeyRotationSource.Automatic, fileKey, fileHash,
                keyIdMasked: string.Empty, failureReason: ex.Message), cancellationToken);

            return new KeyRotationCheckResult(
                KeyRotationCheckOutcome.InvalidFileFormat, bucketName, fileKey, fileHash, Detail: ex.Message);
        }

        var keyIdHint = KeyIdMask.Mask(parsed.AccessKeyId);

        var validation = await credentialValidator.ValidateAsync(
            parsed.AccessKeyId, parsed.SecretAccessKey, bucketName, cancellationToken);

        switch (validation.Outcome)
        {
            case S3CredentialValidationOutcome.InvalidCredentials:
                await repository.AddFailedAsync(CreateRecord(
                    bucketName, KeyRotationSource.Automatic, fileKey, fileHash,
                    keyIdMasked: keyIdHint, failureReason: validation.Detail), cancellationToken);

                return new KeyRotationCheckResult(
                    KeyRotationCheckOutcome.ValidationFailed, bucketName, fileKey, fileHash, keyIdHint, validation.Detail);

            case S3CredentialValidationOutcome.TransientError:
                return new KeyRotationCheckResult(
                    KeyRotationCheckOutcome.TransientError, bucketName, fileKey, fileHash, keyIdHint, validation.Detail);
        }

        var record = CreateValidatedRecord(
            bucketName, KeyRotationSource.Automatic, parsed.AccessKeyId, parsed.SecretAccessKey,
            fileKey: fileKey, fileHash: fileHash);

        await repository.ActivateAsync(record, cancellationToken);
        credentialsProvider.Invalidate();

        return new KeyRotationCheckResult(
            KeyRotationCheckOutcome.Adopted, bucketName, fileKey, fileHash, keyIdHint);
    }

    public async Task<KeyRotationActionResult> ApplyManualAsync(
        string accessKeyId, string secretAccessKey, CancellationToken cancellationToken = default)
    {
        if (!secretProtector.IsConfigured)
        {
            return new KeyRotationActionResult(
                KeyRotationActionOutcome.NotConfigured,
                Detail: $"Encryption key '{options.EncryptionKeySecretName}' is not configured; credentials cannot be stored");
        }

        if (string.IsNullOrWhiteSpace(accessKeyId) || string.IsNullOrWhiteSpace(secretAccessKey))
        {
            return new KeyRotationActionResult(
                KeyRotationActionOutcome.InvalidRequest,
                Detail: "Both an access key id and a secret access key are required");
        }

        var bucketName = s3ClientFactory.GetClientBucketName<ExternalStorageClient>();

        await using var lockHandle = await distributedLock.TryAcquireAsync(
            options.LockName, TimeSpan.FromMinutes(options.LockDurationMinutes), cancellationToken);

        if (lockHandle is null)
        {
            return new KeyRotationActionResult(KeyRotationActionOutcome.LockUnavailable);
        }

        var validation = await credentialValidator.ValidateAsync(accessKeyId, secretAccessKey, bucketName, cancellationToken);
        if (validation.Outcome != S3CredentialValidationOutcome.Valid)
        {
            return ToFailedActionResult(validation);
        }

        var record = CreateValidatedRecord(bucketName, KeyRotationSource.Manual, accessKeyId, secretAccessKey);

        await repository.ActivateAsync(record, cancellationToken);
        credentialsProvider.Invalidate();

        logger.LogInformation("{LogPrefix} Manually applied access key {KeyIdHint} (rotation {RotationId})",
            LogPrefix, record.KeyIdMasked, record.Id);

        return new KeyRotationActionResult(KeyRotationActionOutcome.Applied, record);
    }

    public async Task<KeyRotationActionResult> RollbackAsync(string rotationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rotationId);

        if (!secretProtector.IsConfigured)
        {
            return new KeyRotationActionResult(
                KeyRotationActionOutcome.NotConfigured,
                Detail: $"Encryption key '{options.EncryptionKeySecretName}' is not configured");
        }

        var bucketName = s3ClientFactory.GetClientBucketName<ExternalStorageClient>();

        await using var lockHandle = await distributedLock.TryAcquireAsync(
            options.LockName, TimeSpan.FromMinutes(options.LockDurationMinutes), cancellationToken);

        if (lockHandle is null)
        {
            return new KeyRotationActionResult(KeyRotationActionOutcome.LockUnavailable);
        }

        var target = await repository.GetByIdAsync(rotationId, cancellationToken);
        if (target is null)
        {
            return new KeyRotationActionResult(
                KeyRotationActionOutcome.NotFound, Detail: $"No rotation record found with id '{rotationId}'");
        }

        if (target.EncryptedAccessKeyId is null || target.EncryptedSecretAccessKey is null)
        {
            return new KeyRotationActionResult(
                KeyRotationActionOutcome.InvalidRequest,
                Detail: "The target rotation holds no key material (it is a failed rotation record)");
        }

        if (target.Status == KeyRotationStatus.Active)
        {
            return new KeyRotationActionResult(
                KeyRotationActionOutcome.InvalidRequest, Detail: "The target rotation is already active");
        }

        var accessKeyId = secretProtector.Unprotect(target.EncryptedAccessKeyId, SecretPurposes.AccessKeyId);
        var secretAccessKey = secretProtector.Unprotect(target.EncryptedSecretAccessKey, SecretPurposes.SecretAccessKey);

        var validation = await credentialValidator.ValidateAsync(accessKeyId, secretAccessKey, bucketName, cancellationToken);
        if (validation.Outcome != S3CredentialValidationOutcome.Valid)
        {
            return ToFailedActionResult(validation);
        }

        var record = CreateValidatedRecord(bucketName, KeyRotationSource.Rollback, accessKeyId, secretAccessKey,
            rolledBackFromId: target.Id);

        await repository.ActivateAsync(record, cancellationToken);
        credentialsProvider.Invalidate();

        logger.LogInformation("{LogPrefix} Rolled back to access key {KeyIdHint} from rotation {TargetRotationId} (new rotation {RotationId})",
            LogPrefix, record.KeyIdMasked, target.Id, record.Id);

        return new KeyRotationActionResult(KeyRotationActionOutcome.Applied, record);
    }

    private async Task<byte[]?> TryDownloadFileAsync(string bucketName, string fileKey, CancellationToken cancellationToken)
    {
        var client = s3ClientFactory.GetClient<ExternalStorageClient>();

        try
        {
            using var response = await client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = fileKey
            }, cancellationToken);

            using var buffer = new MemoryStream();
            await response.ResponseStream.CopyToAsync(buffer, cancellationToken);
            return buffer.ToArray();
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound
            || string.Equals(ex.ErrorCode, "NoSuchKey", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }
    }

    private KeyRotationRecord CreateValidatedRecord(
        string bucketName,
        KeyRotationSource source,
        string accessKeyId,
        string secretAccessKey,
        string? fileKey = null,
        string? fileHash = null,
        string? rolledBackFromId = null)
    {
        var now = timeProvider.GetUtcNow().UtcDateTime;
        var record = CreateRecord(bucketName, source, fileKey, fileHash, KeyIdMask.Mask(accessKeyId), failureReason: null);

        record.Status = KeyRotationStatus.Active;
        record.EncryptedAccessKeyId = secretProtector.Protect(accessKeyId, SecretPurposes.AccessKeyId);
        record.EncryptedSecretAccessKey = secretProtector.Protect(secretAccessKey, SecretPurposes.SecretAccessKey);
        record.ValidatedAtUtc = now;
        record.RolledBackFromId = rolledBackFromId;

        return record;
    }

    private KeyRotationRecord CreateRecord(
        string bucketName,
        KeyRotationSource source,
        string? fileKey,
        string? fileHash,
        string keyIdMasked,
        string? failureReason)
    {
        return new KeyRotationRecord
        {
            Id = Guid.NewGuid().ToString(),
            BucketName = bucketName,
            RotatedAtUtc = timeProvider.GetUtcNow().UtcDateTime,
            Source = source,
            Status = KeyRotationStatus.Failed,
            FileKey = fileKey,
            FileHash = fileHash,
            KeyIdMasked = keyIdMasked,
            FailureReason = failureReason
        };
    }

    private static KeyRotationActionResult ToFailedActionResult(S3CredentialValidationResult validation) =>
        validation.Outcome == S3CredentialValidationOutcome.InvalidCredentials
            ? new KeyRotationActionResult(KeyRotationActionOutcome.ValidationFailed, Detail: validation.Detail)
            : new KeyRotationActionResult(KeyRotationActionOutcome.TransientError, Detail: validation.Detail);
}
