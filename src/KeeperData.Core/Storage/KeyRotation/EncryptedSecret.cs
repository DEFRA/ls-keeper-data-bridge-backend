namespace KeeperData.Core.Storage.KeyRotation;

/// <summary>
/// An AES-256-GCM encrypted secret value, stored as base64 fields.
/// </summary>
public record EncryptedSecret
{
    /// <summary>
    /// The version of the encryption key that produced this ciphertext.
    /// Supports future encryption-key rotation with lazy re-encryption.
    /// </summary>
    public required int KeyVersion { get; init; }

    /// <summary>
    /// The 96-bit random nonce, base64 encoded. Unique per encryption.
    /// </summary>
    public required string Nonce { get; init; }

    /// <summary>
    /// The ciphertext, base64 encoded.
    /// </summary>
    public required string CipherText { get; init; }

    /// <summary>
    /// The 128-bit GCM authentication tag, base64 encoded.
    /// </summary>
    public required string Tag { get; init; }
}
