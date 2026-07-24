using Amazon.Runtime;
using KeeperData.Core.Storage.KeyRotation;
using KeeperData.Infrastructure.Storage.Configuration;
using KeeperData.Infrastructure.Storage.KeyRotation.Configuration;
using Microsoft.Extensions.Logging;

namespace KeeperData.Infrastructure.Storage.KeyRotation;

/// <summary>
/// Serves the latest validated rotated credentials from Mongo with a short in-process TTL
/// cache, falling back to the env-var configured credentials when the feature is dormant,
/// no rotation exists, or Mongo/decryption fails. Never throws from <see cref="GetCurrent"/>.
/// </summary>
public sealed class ExternalStorageCredentialsProvider : IExternalStorageCredentialsProvider
{
    private const string LogPrefix = "[KeyRotation]";
    private static readonly TimeSpan FailureCacheDuration = TimeSpan.FromSeconds(30);

    private readonly IKeyRotationRepository _repository;
    private readonly ISecretProtector _secretProtector;
    private readonly ExternalStorageKeyRotationOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<ExternalStorageCredentialsProvider> _logger;
    private readonly ImmutableCredentials _fallback;

    private readonly object _sync = new();
    private ImmutableCredentials? _cached;
    private string? _cachedKeyIdMasked;
    private DateTimeOffset _cacheExpiresAt = DateTimeOffset.MinValue;

    public ExternalStorageCredentialsProvider(
        IKeyRotationRepository repository,
        ISecretProtector secretProtector,
        ExternalStorageKeyRotationOptions options,
        StorageConfiguration storageConfiguration,
        TimeProvider timeProvider,
        ILogger<ExternalStorageCredentialsProvider> logger)
    {
        _repository = repository;
        _secretProtector = secretProtector;
        _options = options;
        _timeProvider = timeProvider;
        _logger = logger;

        var fallbackAccessKey = Environment.GetEnvironmentVariable(storageConfiguration.ExternalStorage.AccessKeySecretName) ?? string.Empty;
        var fallbackSecretKey = Environment.GetEnvironmentVariable(storageConfiguration.ExternalStorage.SecretKeySecretName) ?? string.Empty;
        _fallback = new ImmutableCredentials(fallbackAccessKey, fallbackSecretKey, null);
    }

    public ImmutableCredentials GetCurrent()
    {
        if (!_secretProtector.IsConfigured)
        {
            return _fallback;
        }

        lock (_sync)
        {
            var now = _timeProvider.GetUtcNow();
            if (_cached is not null && now < _cacheExpiresAt)
            {
                return _cached;
            }

            try
            {
                var active = _repository.GetActiveAsync(CancellationToken.None).GetAwaiter().GetResult();

                if (active?.EncryptedAccessKeyId is null || active.EncryptedSecretAccessKey is null)
                {
                    SetCache(_fallback, keyIdMasked: null, now.Add(TimeSpan.FromSeconds(_options.CredentialsCacheSeconds)));
                    return _fallback;
                }

                var accessKeyId = _secretProtector.Unprotect(active.EncryptedAccessKeyId, SecretPurposes.AccessKeyId);
                var secretAccessKey = _secretProtector.Unprotect(active.EncryptedSecretAccessKey, SecretPurposes.SecretAccessKey);
                var credentials = new ImmutableCredentials(accessKeyId, secretAccessKey, null);

                if (_cachedKeyIdMasked != active.KeyIdMasked)
                {
                    _logger.LogInformation(
                        "{LogPrefix} External storage credentials now using rotated key {KeyIdHint} (rotation {RotationId})",
                        LogPrefix, active.KeyIdMasked, active.Id);
                }

                SetCache(credentials, active.KeyIdMasked, now.Add(TimeSpan.FromSeconds(_options.CredentialsCacheSeconds)));
                return credentials;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "{LogPrefix} Failed to resolve rotated credentials; falling back to configured env credentials",
                    LogPrefix);
                SetCache(_fallback, keyIdMasked: null, now.Add(FailureCacheDuration));
                return _fallback;
            }
        }
    }

    public void Invalidate()
    {
        lock (_sync)
        {
            _cached = null;
            _cacheExpiresAt = DateTimeOffset.MinValue;
        }
    }

    private void SetCache(ImmutableCredentials credentials, string? keyIdMasked, DateTimeOffset expiresAt)
    {
        _cached = credentials;
        _cachedKeyIdMasked = keyIdMasked;
        _cacheExpiresAt = expiresAt;
    }
}
