using Amazon;
using Amazon.S3;
using KeeperData.Core.Storage;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Configuration;
using KeeperData.Infrastructure.Storage.Factories;
using KeeperData.Infrastructure.Storage.Factories.Implementations;
using KeeperData.Infrastructure.Storage.KeyRotation;
using KeeperData.Infrastructure.Storage.KeyRotation.Setup;
using KeeperData.Infrastructure.Storage.Readers;
using KeeperData.Infrastructure.Storage.Readers.Implementations;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.Storage.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    public static void AddStorageDependencies(this IServiceCollection services, IConfiguration configuration)
    {
        var storageConfiguration = configuration.GetSection(nameof(StorageConfiguration)).Get<StorageConfiguration>()!;
        services.AddSingleton(storageConfiguration);

        var defaultAmazonS3Config = GetDefaultAmazonS3Config(configuration);
        services.AddSingleton(defaultAmazonS3Config);

        var factory = new S3ClientFactory();

        var externalS3Config = !string.IsNullOrEmpty(storageConfiguration.ExternalStorage.ServiceUrl)
            ? new AmazonS3Config { ServiceURL = storageConfiguration.ExternalStorage.ServiceUrl }
            : defaultAmazonS3Config;

        // The external client uses rotatable credentials: latest validated rotated key from
        // Mongo when key rotation is configured, otherwise the env-var configured fallback.
        // Preserve the original fail-fast on missing fallback credentials.
        var fallbackAccessKey = Environment.GetEnvironmentVariable(storageConfiguration.ExternalStorage.AccessKeySecretName);
        var fallbackSecretKey = Environment.GetEnvironmentVariable(storageConfiguration.ExternalStorage.SecretKeySecretName);

        if (string.IsNullOrWhiteSpace(fallbackAccessKey) || string.IsNullOrWhiteSpace(fallbackSecretKey))
            throw new InvalidOperationException($"Missing AWS credentials for '{nameof(ExternalStorageClient)}'");

        var rotatableCredentials = new RotatableExternalCredentials(fallbackAccessKey, fallbackSecretKey);
        services.AddSingleton(rotatableCredentials);
        services.AddSingleton(new ExternalStorageS3Config(externalS3Config));

        factory.AddClientWithCredentials<ExternalStorageClient>(
                storageConfiguration.ExternalStorage.BucketName,
                rotatableCredentials,
                externalS3Config);

        services.AddKeyRotationDependencies(configuration);

        if (!storageConfiguration.UseFileSystem)
        {
            factory.AddClient<InternalStorageClient>(
                storageConfiguration.InternalStorage.BucketName,
                defaultAmazonS3Config);
        }

        if (storageConfiguration.ExternalStorage.HealthcheckEnabled
            || (!storageConfiguration.UseFileSystem && storageConfiguration.InternalStorage.HealthcheckEnabled))
        {
            services.AddHealthChecks()
                .AddCheck<AwsS3HealthCheck>("aws_s3", tags: ["aws", "s3"]);
        }

        services.AddHealthChecks()
            .AddCheck<InternalStorageHealthCheck>("internal_storage", tags: ["storage"]);

        services.AddSingleton<IS3ClientFactory>(factory);

        services.AddTransient<IStorageReader<ExternalStorageClient>, ExternalStorageReader>();

        if (storageConfiguration.UseFileSystem)
        {
            services.AddTransient<IBlobStorageServiceFactory, FileSystemBlobStorageServiceFactory>();
        }
        else
        {
            services.AddTransient<IBlobStorageServiceFactory, S3BlobStorageServiceFactory>();
        }

    }

    private static AmazonS3Config GetDefaultAmazonS3Config(IConfiguration configuration)
    {
        if (configuration["LOCALSTACK_ENDPOINT"] != null)
        {
            return new AmazonS3Config
            {
                ServiceURL = configuration["LOCALSTACK_ENDPOINT"],
                ForcePathStyle = true
            };
        }

        return new AmazonS3Config
        {
            RegionEndpoint = RegionEndpoint.EUWest2
        };
    }
}