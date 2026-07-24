namespace KeeperData.Infrastructure.Storage.KeyRotation.Configuration;

/// <summary>
/// Configuration for automated external storage access-key rotation.
/// </summary>
public record ExternalStorageKeyRotationOptions
{
    public const string SectionName = "ExternalStorageKeyRotation";

    /// <summary>
    /// Name of the environment variable (CDP secret) holding the base64 encoded 32-byte
    /// AES-256 encryption key used to protect stored credentials. When the variable is not
    /// set the feature is dormant; when set but invalid the service fails fast at startup.
    /// </summary>
    public string EncryptionKeySecretName { get; init; } = "KEY_ROTATION_ENCRYPTION_KEY";

    /// <summary>How long resolved credentials are cached in-process before re-reading Mongo.</summary>
    public int CredentialsCacheSeconds { get; init; } = 300;

    /// <summary>Distributed lock name serialising rotation state changes across instances.</summary>
    public string LockName { get; init; } = "external-storage-key-rotation";

    /// <summary>Duration of the distributed rotation lock, in minutes.</summary>
    public int LockDurationMinutes { get; init; } = 5;
}
