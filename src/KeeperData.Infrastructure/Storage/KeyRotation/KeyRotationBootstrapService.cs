using KeeperData.Core.Storage.KeyRotation;
using KeeperData.Infrastructure.Storage.KeyRotation.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.Storage.KeyRotation;

/// <summary>
/// Connects the Mongo-backed credentials provider to the <see cref="RotatableExternalCredentials"/>
/// used by the external storage S3 client, and logs the key rotation feature status at startup.
/// Until this runs (and whenever no provider is attached) the client serves the env-var
/// fallback credentials, preserving pre-feature behaviour.
/// </summary>
[ExcludeFromCodeCoverage(Justification = "Startup glue - behaviour covered by provider/service tests.")]
public sealed class KeyRotationBootstrapService(
    RotatableExternalCredentials rotatableCredentials,
    IExternalStorageCredentialsProvider credentialsProvider,
    ISecretProtector secretProtector,
    ExternalStorageKeyRotationOptions options,
    ILogger<KeyRotationBootstrapService> logger) : IHostedService
{
    private const string LogPrefix = "[KeyRotation]";

    public Task StartAsync(CancellationToken cancellationToken)
    {
        rotatableCredentials.AttachProvider(credentialsProvider);

        if (secretProtector.IsConfigured)
        {
            logger.LogInformation(
                "{LogPrefix} Automated external storage key rotation is ACTIVE (encryption key '{SecretName}' configured); " +
                "rotated keys from Mongo take precedence over configured env credentials",
                LogPrefix, options.EncryptionKeySecretName);
        }
        else
        {
            logger.LogInformation(
                "{LogPrefix} Automated external storage key rotation is DORMANT: encryption key '{SecretName}' is not configured; " +
                "the service uses the configured env credentials as before",
                LogPrefix, options.EncryptionKeySecretName);
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
