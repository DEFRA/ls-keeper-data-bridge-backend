using KeeperData.Core.Domain.Entities;

namespace KeeperData.Core.Storage.KeyRotation;

/// <summary>
/// Orchestrates external storage access-key rotation: the scheduled daily check,
/// manual application of credentials, and rollback to a previous rotation.
/// </summary>
public interface IKeyRotationService
{
    /// <summary>
    /// The daily check: derive the rotation file name, detect content changes by hash,
    /// parse, validate against the bucket, and adopt the new credentials when valid.
    /// Never throws for expected outcomes — inspect the result.
    /// </summary>
    Task<KeyRotationCheckResult> CheckAndRotateAsync(CancellationToken cancellationToken = default);

    /// <summary>Validates and activates manually supplied credentials.</summary>
    Task<KeyRotationActionResult> ApplyManualAsync(string accessKeyId, string secretAccessKey, CancellationToken cancellationToken = default);

    /// <summary>
    /// Re-validates the credentials captured in rotation <paramref name="rotationId"/> and,
    /// when they still authenticate, activates them as a new rollback rotation.
    /// </summary>
    Task<KeyRotationActionResult> RollbackAsync(string rotationId, CancellationToken cancellationToken = default);
}

/// <summary>Outcome of the scheduled daily rotation check.</summary>
public enum KeyRotationCheckOutcome
{
    /// <summary>No encryption key is configured; the feature is dormant.</summary>
    NotConfigured,

    /// <summary>Another instance holds the rotation lock.</summary>
    LockUnavailable,

    /// <summary>No rotation file exists in the bucket.</summary>
    FileNotFound,

    /// <summary>The rotation file's hash matches the most recently processed file.</summary>
    AlreadyProcessed,

    /// <summary>New credentials were validated and adopted.</summary>
    Adopted,

    /// <summary>The rotation file could not be parsed. Recorded as failed.</summary>
    InvalidFileFormat,

    /// <summary>The new credentials failed validation against the bucket. Recorded as failed.</summary>
    ValidationFailed,

    /// <summary>A transient error (network/service) occurred. Nothing recorded; retried next run.</summary>
    TransientError
}

/// <summary>Result of the scheduled daily rotation check.</summary>
public sealed record KeyRotationCheckResult(
    KeyRotationCheckOutcome Outcome,
    string BucketName,
    string? FileKey = null,
    string? FileHash = null,
    string? KeyIdHint = null,
    string? Detail = null);

/// <summary>Outcome of a manual apply or rollback action.</summary>
public enum KeyRotationActionOutcome
{
    /// <summary>The credentials were validated and activated.</summary>
    Applied,

    /// <summary>The referenced rotation record does not exist.</summary>
    NotFound,

    /// <summary>The request is not actionable (e.g. rollback target is already active or holds no key material).</summary>
    InvalidRequest,

    /// <summary>The credentials failed validation against the bucket. Nothing was changed.</summary>
    ValidationFailed,

    /// <summary>A transient error (network/service) prevented validation. Nothing was changed; retry.</summary>
    TransientError,

    /// <summary>No encryption key is configured; credentials cannot be stored.</summary>
    NotConfigured,

    /// <summary>The rotation lock could not be acquired.</summary>
    LockUnavailable
}

/// <summary>Result of a manual apply or rollback action.</summary>
public sealed record KeyRotationActionResult(
    KeyRotationActionOutcome Outcome,
    KeyRotationRecord? Record = null,
    string? Detail = null);
