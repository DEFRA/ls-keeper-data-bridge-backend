using KeeperData.Core.Reporting.Impl;
using KeeperData.Core.Reporting.Services;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reporting.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    public static void AddReportingDependencies(this IServiceCollection services)
    {
        // Register lineage services
        services.AddSingleton<ILineageIdGenerator, LineageIdGenerator>();
        services.AddSingleton<ILineageMapper, LineageMapper>();
        services.AddSingleton<ILineageIndexManagerFactory, LineageIndexManagerFactory>();

        // Register reporting service
        services.AddScoped<IImportReportingService, ImportReportingService>();

        // Register reporting collection management service
        services.AddScoped<IReportingCollectionManagementService, ReportingCollectionManagementService>();
    }
}