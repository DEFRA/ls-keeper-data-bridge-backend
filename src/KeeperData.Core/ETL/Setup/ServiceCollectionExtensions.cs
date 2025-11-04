using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.ETL.Utils;
using KeeperData.Core.Reporting.Setup;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.ETL.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    public static void AddEtlDependencies(this IServiceCollection services)
    {
        services.AddSingleton<IDataSetDefinitions>(_ => StandardDataSetDefinitionsBuilder.Build());
        services.AddTransient<CsvRowCounter>();
        services.AddTransient<IExternalCatalogueServiceFactory, ExternalCatalogueServiceFactory>();
        services.AddTransient<IIngestionPipeline, IngestionPipeline>();
        services.AddTransient<IAcquisitionPipeline, AcquisitionPipeline>();
        services.AddTransient<IImportOrchestrator, ImportOrchestrator>();

        // Add reporting dependencies
        services.AddReportingDependencies();
    }
}