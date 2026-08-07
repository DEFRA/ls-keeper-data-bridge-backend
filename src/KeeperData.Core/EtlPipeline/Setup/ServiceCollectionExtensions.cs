using KeeperData.Core.EtlPipeline.Snapshots;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.Pipeline;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using XsvHcdtHelper;

namespace KeeperData.Core.EtlPipeline.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddEtlPipeline(this IServiceCollection services)
    {
        services.TryAddSingleton(TimeProvider.System);

        services.AddScoped<IPipelineExecutor, PipelineExecutor>();
        services.AddScoped<IEtlPipelineFactory, EtlPipelineFactory>();

        services.TryAddScoped<IDeltaMergeEngine, ParquetDeltaMergeEngine>();

        services.AddScoped<DecryptStage>();
        services.AddScoped<SnapshotStage>();
        services.AddScoped<LoadDuckDbStage>();

        
        services.AddXsvHcdtHelper();

        return services;
    }
}
