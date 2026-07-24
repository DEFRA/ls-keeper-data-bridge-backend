using Amazon.S3;
using Amazon.S3.Model;
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
/// and recorded append-only via <see cref="IKeyRotationStore"/>.
/// </summary>
public sealed class KeyRotationService(
    IS3ClientFactory s3ClientFactory,
    IKeyRotationStore store,
    IS3CredentialValidator credentialValidator,
    IDistributedLock distributedLock,
    ExternalStorageKeyRotationOptions options,
    ILogger<KeyRotationService> logger) : IKeyRotationService
{
    private const string LogPrefix = "[KeyRotation]";

    public async Task<KeyRotationCheckResult> CheckAndRotateAsync(CancellationToken cancellationToken = default)
    {
        var bucketName = s3ClientFactory.GetClientBucketName<ExternalStorageClient>();

        if (!store.IsEncryptionConfigured)
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

        var lastObservedHash = await store.GetLatestObservedFileHashAsync(cancellationToken);
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
            await store.RecordFailedAsync(new FailedRotation(
                bucketName, fileKey, fileHash, KeyIdMasked: string.Empty, FailureReason: ex.Message), cancellationToken);

            return new KeyRotationCheckResult(
                KeyRotationCheckOutcome.InvalidFileFormat, bucketName, fileKey, fileHash, Detail: ex.Message);
        }

        var keyIdHint = KeyIdMask.Mask(parsed.AccessKeyId);

        var validation = await credentialValidator.ValidateAsync(
            parsed.AccessKeyId, parsed.SecretAccessKey, bucketName, cancellationToken);

        switch (validation.Outcome)
        {
            case S3CredentialValidationOutcome.InvalidCredentials:
                await store.RecordFailedAsync(new FailedRotation(
                    bucketName, fileKey, fileHash, KeyIdMasked: keyIdHint, FailureReason: validation.Detail), cancellationToken);

                return new KeyRotationCheckResult(
                    KeyRotationCheckOutcome.ValidationFailed, bucketName, fileKey, fileHash, keyIdHint, validation.Detail);

            case S3CredentialValidationOutcome.TransientError:
                return new KeyRotationCheckResult(
                    KeyRotationCheckOutcome.TransientError, bucketName, fileKey, fileHash, keyIdHint, validation.Detail);
        }

        await store.ActivateValidatedAsync(new ValidatedRotation(
            bucketName, KeyRotationSource.Automatic, parsed.AccessKeyId, parsed.SecretAccessKey,
            FileKey: fileKey, FileHash: fileHash), cancellationToken);

        return new KeyRotationCheckResult(
            KeyRotationCheckOutcome.Adopted, bucketName, fileKey, fileHash, keyIdHint);
    }

    public async Task<KeyRotationActionResult> ApplyManualAsync(
        string accessKeyId, string secretAccessKey, CancellationToken cancellationToken = default)
    {
        if (!store.IsEncryptionConfigured)
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

        var record = await store.ActivateValidatedAsync(new ValidatedRotation(
            bucketName, KeyRotationSource.Manual, accessKeyId, secretAccessKey), cancellationToken);

        logger.LogInformation("{LogPrefix} Manually applied access key {KeyIdHint} (rotation {RotationId})",
            LogPrefix, record.KeyIdMasked, record.Id);

        return new KeyRotationActionResult(KeyRotationActionOutcome.Applied, record);
    }

    public async Task<KeyRotationActionResult> RollbackAsync(string rotationId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rotationId);

        if (!store.IsEncryptionConfigured)
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

        var target = await store.GetByIdAsync(rotationId, cancellationToken);
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

        var (accessKeyId, secretAccessKey) = store.DecryptCredentials(target);

        var validation = await credentialValidator.ValidateAsync(accessKeyId, secretAccessKey, bucketName, cancellationToken);
        if (validation.Outcome != S3CredentialValidationOutcome.Valid)
        {
            return ToFailedActionResult(validation);
        }

        var record = await store.ActivateValidatedAsync(new ValidatedRotation(
            bucketName, KeyRotationSource.Rollback, accessKeyId, secretAccessKey,
            RolledBackFromId: target.Id), cancellationToken);

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

    private static KeyRotationActionResult ToFailedActionResult(S3CredentialValidationResult validation) =>
        validation.Outcome == S3CredentialValidationOutcome.InvalidCredentials
            ? new KeyRotationActionResult(KeyRotationActionOutcome.ValidationFailed, Detail: validation.Detail)
            : new KeyRotationActionResult(KeyRotationActionOutcome.TransientError, Detail: validation.Detail);
}
