using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.Pipeline;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KeeperData.Core.EtlPipeline.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddEtlPipeline(this IServiceCollection services)
    {
        services.TryAddSingleton(TimeProvider.System);

        services.AddScoped<IPipelineExecutor, PipelineExecutor>();
        services.AddScoped<IEtlPipelineFactory, EtlPipelineFactory>();

        services.AddScoped<SnapshotStage>();

        return services;
    }
}
