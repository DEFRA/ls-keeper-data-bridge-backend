using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Setup;
using KeeperData.Core.EtlPipeline.Staging;
using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.EtlPipeline.Views;
using KeeperData.Core.Pipeline;
using KeeperData.Etl.Tool.Configuration;
using KeeperData.Etl.Tool.Rendering;
using KeeperData.Etl.Tool.Status;
using KeeperData.Infrastructure.Crypto;
using KeeperData.Infrastructure.EtlPipeline.Staging;
using KeeperData.Infrastructure.EtlPipeline.Storage;
using KeeperData.Infrastructure.EtlPipeline.Views;
using KeeperData.Infrastructure.Storage.Setup;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KeeperData.Etl.Tool.Setup;

/// <summary>Composes the ETL pipeline and nothing else.
///
/// The pipeline's only metadata dependency is <see cref="IEtlImportStatusStore"/>, so swapping that
/// for SQLite leaves Mongo out entirely - the reporting, cleanse, throttling and key-rotation
/// repositories all belong to the API and the legacy ingestion path, neither of which runs here.
///
/// Everything below the substitutions is the production registration, so what this runs is the
/// pipeline the service runs, not a copy of it.</summary>
internal static class EtlToolServices
{
    public static IServiceCollection AddEtlTool(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<EtlToolOptions>(configuration.GetSection(EtlToolOptions.SectionName));
        services.AddSingleton(TimeProvider.System);

        services.AddSingleton<IDataSetDefinitions>(_ =>
            StandardDataSetDefinitionsBuilder.Build(configuration["StorageConfiguration:SourceExternalDataSetFolder"]));

        // The production registration: the external source stays on S3 while UseFileSystem puts
        // every internal container on disk. Its key-rotation dependencies are registered but never
        // resolved - rotation runs from a hosted service, and the tool never starts the host.
        services.AddStorageDependencies(configuration);
        services.AddSingleton<IEtlPipelineStorageProvider, FileSystemEtlPipelineStorageProvider>();

        services.Configure<DuckDbConfiguration>(options =>
        {
            var tool = configuration.GetSection(EtlToolOptions.SectionName).Get<EtlToolOptions>() ?? new EtlToolOptions();

            if (SqliteExtensionResolver.Resolve(tool.SqliteExtensionPath) is { Length: > 0 } path)
            {
                options.SqliteExtensionPath = path;
            }

            options.MemoryLimit = tool.MemoryLimit;
        });

        services.AddScoped<IStagingDatabaseWriter, DuckDbStagingDatabaseWriter>();
        services.AddScoped<ISqliteViewWriter, DuckDbSqliteViewWriter>();

        services.AddCrypto(configuration);
        services.AddTransient<IExternalCatalogueServiceFactory, ExternalCatalogueServiceFactory>();

        services.AddEtlPipeline();

        services.AddSingleton<IEtlImportStatusStore, SqliteEtlImportStatusStore>();
        services.AddScoped<IPipelineRunObserver, EtlImportStatusObserver>();
        services.AddScoped<IPipelineRunObserver, ConsoleProgressObserver>();

        return services;    }
}
