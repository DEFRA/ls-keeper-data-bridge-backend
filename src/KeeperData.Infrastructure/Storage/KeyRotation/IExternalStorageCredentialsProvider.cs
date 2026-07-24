using Amazon.Runtime;

namespace KeeperData.Infrastructure.Storage.KeyRotation;

/// <summary>
/// Resolves the credentials the external storage S3 client should currently use:
/// the latest validated rotated key from Mongo, or the env-var configured fallback.
/// Implementations must never throw from <see cref="GetCurrent"/> — on any failure
/// they fall back to the configured credentials.
/// </summary>
public interface IExternalStorageCredentialsProvider
{
    /// <summary>Gets the credentials to use right now (cached with a short TTL).</summary>
    ImmutableCredentials GetCurrent();

    /// <summary>
    /// Drops the in-process cache so the next request re-reads the active rotation record.
    /// Called immediately after a rotation is adopted locally.
    /// </summary>
    void Invalidate();
}
