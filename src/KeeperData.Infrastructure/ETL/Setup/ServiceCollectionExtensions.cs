using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.ETL.Export;
using Microsoft.Extensions.DependencyInjection;

namespace KeeperData.Infrastructure.ETL.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    public static void AddEtlServices(this IServiceCollection services)
    {
        services.AddScoped<ICphExportService, CphExportService>();
        services.AddScoped<ICphExportStatusService, CphExportStatusService>();
    }
}
