using KeeperData.Core.Storage.KeyRotation;
using Microsoft.Extensions.Logging;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Worker.Tasks.Implementations;

/// <summary>
/// Daily external storage key rotation check. Every run logs an outcome with the
/// <c>[KeyRotation]</c> prefix so the 3am activity is always visible in the logs,
/// whether a key file was processed, unchanged, missing, or the feature is dormant.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Thin logging wrapper over IKeyRotationService - covered by service tests.")]
public class TaskRotateExternalStorageKeys(
    IKeyRotationService keyRotationService,
    ILogger<TaskRotateExternalStorageKeys> logger) : ITaskRotateExternalStorageKeys
{
    private const string LogPrefix = "[KeyRotation]";

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("{LogPrefix} Daily key rotation check starting", LogPrefix);

        var result = await keyRotationService.CheckAndRotateAsync(cancellationToken);

        switch (result.Outcome)
        {
            case KeyRotationCheckOutcome.NotConfigured:
                logger.LogInformation(
                    "{LogPrefix} Check skipped - {Detail}; the service continues to use the configured env credentials (bucket {BucketName})",
                    LogPrefix, result.Detail, result.BucketName);
                break;

            case KeyRotationCheckOutcome.LockUnavailable:
                logger.LogInformation(
                    "{LogPrefix} Check skipped - another instance holds the rotation lock (bucket {BucketName})",
                    LogPrefix, result.BucketName);
                break;

            case KeyRotationCheckOutcome.FileNotFound:
                logger.LogInformation(
                    "{LogPrefix} No key file found in bucket {BucketName} (expected {FileKey}) - no action taken",
                    LogPrefix, result.BucketName, result.FileKey);
                break;

            case KeyRotationCheckOutcome.AlreadyProcessed:
                logger.LogInformation(
                    "{LogPrefix} Key file {FileKey} unchanged (hash {FileHash}) - already processed, no action taken",
                    LogPrefix, result.FileKey, result.FileHash);
                break;

            case KeyRotationCheckOutcome.Adopted:
                logger.LogInformation(
                    "{LogPrefix} ROTATED: adopted new access key {KeyIdHint} from {FileKey} (hash {FileHash}) after successful validation against bucket {BucketName}",
                    LogPrefix, result.KeyIdHint, result.FileKey, result.FileHash, result.BucketName);
                break;

            case KeyRotationCheckOutcome.InvalidFileFormat:
                logger.LogError(
                    "{LogPrefix} Key file {FileKey} (hash {FileHash}) could not be parsed and was recorded as failed: {Detail}",
                    LogPrefix, result.FileKey, result.FileHash, result.Detail);
                break;

            case KeyRotationCheckOutcome.ValidationFailed:
                logger.LogError(
                    "{LogPrefix} New access key {KeyIdHint} from {FileKey} (hash {FileHash}) FAILED validation against bucket {BucketName} and was recorded as failed: {Detail}",
                    LogPrefix, result.KeyIdHint, result.FileKey, result.FileHash, result.BucketName, result.Detail);
                break;

            case KeyRotationCheckOutcome.TransientError:
                logger.LogWarning(
                    "{LogPrefix} Check for {FileKey} in bucket {BucketName} hit a transient error and will retry on the next run: {Detail}",
                    LogPrefix, result.FileKey, result.BucketName, result.Detail);
                break;
        }

        logger.LogInformation("{LogPrefix} Daily key rotation check finished with outcome {Outcome}",
            LogPrefix, result.Outcome);
    }
}
