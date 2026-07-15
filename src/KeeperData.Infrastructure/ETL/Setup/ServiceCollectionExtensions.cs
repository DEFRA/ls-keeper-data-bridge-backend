using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.ETL.Export;
using KeeperData.Core.Pipeline;
using Microsoft.Extensions.DependencyInjection;

namespace KeeperData.Infrastructure.ETL.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    public static void AddFileBasedEtlServices(this IServiceCollection services)
    {
        services.AddScoped<IPipelineExecutor, PipelineExecutor>();
        services.AddScoped<ICphExportService, PipelineCphExportService>();
        services.AddScoped<ICphExportStatusService, CphExportStatusService>();
    }
}
