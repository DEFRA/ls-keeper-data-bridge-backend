using Amazon.S3;
using KeeperData.Core.Crypto;
using KeeperData.Core.Database;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Impl;
using KeeperData.Core.Reporting;
using KeeperData.Core.Reporting.Impl;
using KeeperData.Core.Storage;
using KeeperData.Infrastructure.Crypto;
using KeeperData.Infrastructure.Database.Configuration;
using KeeperData.Infrastructure.Storage;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Configuration;
using KeeperData.Infrastructure.Storage.Factories;
using KeeperData.Infrastructure.Storage.Factories.Implementations;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace KeeperData.Bridge.Tests.Integration.Helpers;

public static class TestServiceProviderBuilder
{
    public static ServiceProvider BuildServiceProvider(
        IAmazonS3 s3Client,
        string bucketName,
        string sourcePrefix,
        string destPrefix,
        IMongoClient mongoClient,
        string mongoDatabaseName,
        DataSetDefinitions dataSetDefinitions)
    {
        var services = new ServiceCollection();

        services.AddLogging(builder => builder.AddConsole());

        services.AddSingleton(TimeProvider.System);

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["AesSalt"] = "Jr8Lm2PXzd7qNbVyWutRfGBxhkHTpE"
            })
            .Build();
        services.AddSingleton<IConfiguration>(configuration);

        var mongoConfig = new MongoConfig
        {
            DatabaseName = mongoDatabaseName,
            DatabaseUri = "not-used-in-test",
            EnableTransactions = false,
            HealthcheckEnabled = false
        };
        
        services.AddSingleton<IOptions<MongoConfig>>(Options.Create(mongoConfig));
        services.AddSingleton<IOptions<IDatabaseConfig>>(Options.Create<IDatabaseConfig>(mongoConfig));
        services.AddSingleton(mongoClient);

        services.AddSingleton<IDataSetDefinitions>(dataSetDefinitions);

        var storageConfig = new StorageConfiguration
        {
            ExternalStorage = new StorageWithCredentialsConfiguration
            {
                BucketName = bucketName,
                AccessKeySecretName = "not-used",
                SecretKeySecretName = "not-used",
                HealthcheckEnabled = false
            },
            InternalStorage = new StorageConfigurationDetails
            {
                BucketName = bucketName,
                HealthcheckEnabled = false
            },
            SourceExternalPrefix = sourcePrefix,
            SourceInternalPrefix = destPrefix,
            TargetInternalPrefix = destPrefix
        };
        services.AddSingleton(storageConfig);

        var s3Config = new AmazonS3Config
        {
            ServiceURL = "http://localhost:4566",
            ForcePathStyle = true,
            UseHttp = true
        };
        services.AddSingleton(s3Config);

        var s3ClientFactory = new S3ClientFactory();
        s3ClientFactory.RegisterMockClient<ExternalStorageClient>(bucketName, s3Client);
        s3ClientFactory.RegisterMockClient<InternalStorageClient>(bucketName, s3Client);
        services.AddSingleton<IS3ClientFactory>(s3ClientFactory);

        services.AddTransient<IBlobStorageServiceFactory, S3BlobStorageServiceFactory>();

        services.AddSingleton<IPasswordSaltService, PasswordSaltService>();
        services.AddSingleton<IAesCryptoTransform, AesCryptoTransform>();

        services.AddScoped<IImportReportingService, ImportReportingService>();

        services.AddTransient<IExternalCatalogueServiceFactory, ExternalCatalogueServiceFactory>();

        services.AddScoped<IAcquisitionPipeline, AcquisitionPipeline>();
        services.AddScoped<IIngestionPipeline, IngestionPipeline>();

        services.AddScoped<IImportOrchestrator, ImportOrchestrator>();

        services.AddScoped<IMongoQueryService, MongoQueryService>();

        return services.BuildServiceProvider();
    }
}
