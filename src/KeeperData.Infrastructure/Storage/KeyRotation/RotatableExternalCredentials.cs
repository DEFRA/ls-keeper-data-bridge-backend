using Amazon.Runtime;

namespace KeeperData.Infrastructure.Storage.KeyRotation;

/// <summary>
/// An <see cref="AWSCredentials"/> implementation that lets the long-lived external storage
/// <c>AmazonS3Client</c> pick up rotated access keys per request without being recreated.
/// The AWS SDK calls <see cref="GetCredentials"/> when signing each request.
/// Until a provider is attached (during host startup) the env-var fallback credentials are
/// served, preserving pre-rotation behaviour.
/// </summary>
public sealed class RotatableExternalCredentials(string fallbackAccessKeyId, string fallbackSecretAccessKey) : AWSCredentials
{
    private readonly ImmutableCredentials _fallback = new(fallbackAccessKeyId, fallbackSecretAccessKey, null);
    private volatile IExternalStorageCredentialsProvider? _provider;

    /// <summary>Attaches the Mongo-backed provider once the DI container is built.</summary>
    public void AttachProvider(IExternalStorageCredentialsProvider provider)
    {
        ArgumentNullException.ThrowIfNull(provider);
        _provider = provider;
    }

    public override ImmutableCredentials GetCredentials() => _provider?.GetCurrent() ?? _fallback;

    public override Task<ImmutableCredentials> GetCredentialsAsync() => Task.FromResult(GetCredentials());
}
