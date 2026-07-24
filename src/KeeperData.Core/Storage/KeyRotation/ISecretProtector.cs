namespace KeeperData.Core.Storage.KeyRotation;

/// <summary>
/// Authenticated encryption for secrets at rest (AES-256-GCM).
/// </summary>
public interface ISecretProtector
{
    /// <summary>
    /// Whether an encryption key is configured. When false the key rotation feature is
    /// dormant and callers must not attempt to protect or unprotect values.
    /// </summary>
    bool IsConfigured { get; }

    /// <summary>The version of the currently configured encryption key.</summary>
    int KeyVersion { get; }

    /// <summary>
    /// Encrypts <paramref name="plainText"/>. The <paramref name="purpose"/> is bound into the
    /// authentication tag (as associated data) so ciphertexts cannot be swapped between fields.
    /// </summary>
    EncryptedSecret Protect(string plainText, string purpose);

    /// <summary>
    /// Decrypts <paramref name="secret"/>, verifying integrity and the <paramref name="purpose"/> binding.
    /// </summary>
    string Unprotect(EncryptedSecret secret, string purpose);
}
