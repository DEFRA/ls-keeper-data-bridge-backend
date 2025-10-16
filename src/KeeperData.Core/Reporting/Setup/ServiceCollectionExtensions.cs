using KeeperData.Core.Reporting.Impl;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Core.Reporting.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    public static void AddReportingDependencies(this IServiceCollection services)
    {
        services.AddScoped<IImportReportingService, ImportReportingService>();
    }
}