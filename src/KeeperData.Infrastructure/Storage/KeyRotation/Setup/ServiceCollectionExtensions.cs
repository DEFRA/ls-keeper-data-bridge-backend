using KeeperData.Core.Storage.KeyRotation;
using KeeperData.Infrastructure.Crypto;
using KeeperData.Infrastructure.Storage.KeyRotation.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.Storage.KeyRotation.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers external storage key rotation: encrypted credential storage, the
    /// rotating credential chain, and the rotation orchestration service.
    /// Fails fast when the encryption key env var is set but invalid; when the env var
    /// is absent the feature is dormant and the service behaves exactly as before.
    /// </summary>
    public static void AddKeyRotationDependencies(this IServiceCollection services, IConfiguration configuration)
    {
        var options = configuration.GetSection(ExternalStorageKeyRotationOptions.SectionName)
            .Get<ExternalStorageKeyRotationOptions>() ?? new ExternalStorageKeyRotationOptions();
        services.AddSingleton(options);

        // Fail-fast: throws at startup when the key is present but not valid base64 / not 32 bytes.
        var secretProtector = AesGcmSecretProtector.FromEnvironment(options.EncryptionKeySecretName);
        services.AddSingleton<ISecretProtector>(secretProtector);

        services.TryAddSingleton(TimeProvider.System);
        services.AddSingleton<IKeyRotationRepository, KeyRotationRepository>();
        services.AddSingleton<IExternalStorageCredentialsProvider, ExternalStorageCredentialsProvider>();
        services.AddSingleton<IS3CredentialValidator, S3CredentialValidator>();
        services.AddSingleton<IKeyRotationService, KeyRotationService>();

        services.AddHostedService<KeyRotationBootstrapService>();
    }
}
