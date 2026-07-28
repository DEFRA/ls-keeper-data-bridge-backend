using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Infrastructure.EtlPipeline.Storage;
using KeeperData.Infrastructure.Storage.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KeeperData.Infrastructure.EtlPipeline.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    /// <summary>Registers storage access for the file-based ETL pipeline folders.
    /// Must be called after AddStorageDependencies, which registers StorageConfiguration
    /// and the S3 client factory this depends on.</summary>
    public static IServiceCollection AddEtlPipelineStorageProvider(this IServiceCollection services, IConfiguration configuration)
    {
        var storageConfiguration = configuration.GetSection(nameof(StorageConfiguration)).Get<StorageConfiguration>()!;

        if (storageConfiguration.UseFileSystem)
        {
            services.AddTransient<IEtlPipelineStorageProvider, FileSystemEtlPipelineStorageProvider>();
        }
        else
        {
            services.AddTransient<IEtlPipelineStorageProvider, S3EtlPipelineStorageProvider>();
        }

        return services;
    }
}
