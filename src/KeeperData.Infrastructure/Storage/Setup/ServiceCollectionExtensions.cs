using Amazon;
using Amazon.S3;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Configuration;
using KeeperData.Infrastructure.Storage.Factories;
using KeeperData.Infrastructure.Storage.Factories.Implementations;
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

        var defaultAmazonS3Config = GetDefaultAmazonS3Config();
        services.AddSingleton(defaultAmazonS3Config);

        var factory = new S3ClientFactory();

        factory.AddClientWithCredentials<ExternalStorageClient>(
                storageConfiguration.ExternalStorage.BucketName,
                storageConfiguration.ExternalStorage.AccessKeySecretName,
                storageConfiguration.ExternalStorage.SecretKeySecretName,
                defaultAmazonS3Config);

        if (storageConfiguration.ExternalStorage.HealthcheckEnabled)
        {
            services.AddHealthChecks()
                .AddCheck<AwsS3HealthCheck>("aws_s3", tags: ["aws", "s3"]);
        }

        services.AddSingleton<IS3ClientFactory>(factory);

        services.AddTransient<IStorageReader<ExternalStorageClient>, ExternalStorageReader>();
    }

    private static AmazonS3Config GetDefaultAmazonS3Config()
    {
        var localStackServiceURL = Environment.GetEnvironmentVariable("LocalStack_ServiceURL");

        if (localStackServiceURL == null)
        {
            return new AmazonS3Config
            {
                RegionEndpoint = RegionEndpoint.EUWest2
            };
        }

        return new AmazonS3Config
        {
            ServiceURL = localStackServiceURL,
            ForcePathStyle = true
        };
    }
}