using KeeperData.Core.Pipeline;
using Microsoft.Extensions.DependencyInjection;

namespace KeeperData.Core.EtlPipeline.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddEtlPipeline(this IServiceCollection services)
    {
        services.AddScoped<IPipelineExecutor, PipelineExecutor>();
        services.AddScoped<IEtlPipelineFactory, EtlPipelineFactory>();

        return services;
    }
}
