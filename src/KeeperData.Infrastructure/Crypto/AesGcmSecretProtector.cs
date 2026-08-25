using KeeperData.Core.Storage.KeyRotation;
using System.Security.Cryptography;
using System.Text;

namespace KeeperData.Infrastructure.Crypto;

/// <summary>
/// AES-256-GCM implementation of <see cref="ISecretProtector"/> for secrets at rest.
/// Each value is encrypted with a fresh 96-bit random nonce and a 128-bit authentication
/// tag; the purpose string is bound as associated data so ciphertexts cannot be swapped
/// between fields. Deliberately separate from <see cref="AesCryptoTransform"/> (AES-ECB),
/// which exists for legacy bulk-file compatibility and is not suitable for secret storage.
/// </summary>
public sealed class AesGcmSecretProtector : ISecretProtector
{
    public const int CurrentKeyVersion = 1;
    private const int KeySizeBytes = 32;
    private const int NonceSizeBytes = 12;
    private const int TagSizeBytes = 16;

    private readonly byte[]? _key;

    private AesGcmSecretProtector(byte[]? key)
    {
        _key = key;
    }

    /// <summary>
    /// Creates a protector from the environment variable named <paramref name="encryptionKeySecretName"/>.
    /// Missing/empty → an unconfigured (dormant) protector.
    /// Present but invalid → <see cref="InvalidOperationException"/> (fail fast at startup).
    /// </summary>
    public static AesGcmSecretProtector FromEnvironment(string encryptionKeySecretName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(encryptionKeySecretName);

        var value = Environment.GetEnvironmentVariable(encryptionKeySecretName);
        if (string.IsNullOrWhiteSpace(value))
        {
            return Unconfigured();
        }

        byte[] key;
        try
        {
            key = Convert.FromBase64String(value.Trim());
        }
        catch (FormatException ex)
        {
            throw new InvalidOperationException(
                $"The key rotation encryption key in environment variable '{encryptionKeySecretName}' is not valid base64.", ex);
        }

        return FromKey(key, encryptionKeySecretName);
    }

    /// <summary>Creates a protector from raw key bytes (must be 32 bytes).</summary>
    public static AesGcmSecretProtector FromKey(byte[] key, string? sourceName = null)
    {
        ArgumentNullException.ThrowIfNull(key);

        if (key.Length != KeySizeBytes)
        {
            var source = sourceName is null ? string.Empty : $" in environment variable '{sourceName}'";
            throw new InvalidOperationException(
                $"The key rotation encryption key{source} must decode to exactly {KeySizeBytes} bytes but was {key.Length} bytes.");
        }

        return new AesGcmSecretProtector(key);
    }

    /// <summary>Creates an unconfigured (dormant) protector.</summary>
    public static AesGcmSecretProtector Unconfigured() => new((byte[]?)null);

    public bool IsConfigured => _key is not null;

    public int KeyVersion => CurrentKeyVersion;

    public EncryptedSecret Protect(string plainText, string purpose)
    {
        ArgumentNullException.ThrowIfNull(plainText);
        ArgumentException.ThrowIfNullOrWhiteSpace(purpose);

        var key = RequireKey();
        var nonce = RandomNumberGenerator.GetBytes(NonceSizeBytes);
        var plainBytes = Encoding.UTF8.GetBytes(plainText);
        var cipherText = new byte[plainBytes.Length];
        var tag = new byte[TagSizeBytes];

        using var aesGcm = new AesGcm(key, TagSizeBytes);
        aesGcm.Encrypt(nonce, plainBytes, cipherText, tag, Encoding.UTF8.GetBytes(purpose));

        return new EncryptedSecret
        {
            KeyVersion = CurrentKeyVersion,
            Nonce = Convert.ToBase64String(nonce),
            CipherText = Convert.ToBase64String(cipherText),
            Tag = Convert.ToBase64String(tag)
        };
    }

    public string Unprotect(EncryptedSecret secret, string purpose)
    {
        ArgumentNullException.ThrowIfNull(secret);
        ArgumentException.ThrowIfNullOrWhiteSpace(purpose);

        var key = RequireKey();

        if (secret.KeyVersion != CurrentKeyVersion)
        {
            throw new InvalidOperationException(
                $"Cannot decrypt secret: it was encrypted with key version {secret.KeyVersion} but the configured key is version {CurrentKeyVersion}.");
        }

        var nonce = Convert.FromBase64String(secret.Nonce);
        var cipherText = Convert.FromBase64String(secret.CipherText);
        var tag = Convert.FromBase64String(secret.Tag);
        var plainBytes = new byte[cipherText.Length];

        using var aesGcm = new AesGcm(key, TagSizeBytes);
        aesGcm.Decrypt(nonce, cipherText, tag, plainBytes, Encoding.UTF8.GetBytes(purpose));

        return Encoding.UTF8.GetString(plainBytes);
    }

    private byte[] RequireKey() =>
        _key ?? throw new InvalidOperationException(
            "The key rotation encryption key is not configured; secrets cannot be protected or unprotected.");
}
