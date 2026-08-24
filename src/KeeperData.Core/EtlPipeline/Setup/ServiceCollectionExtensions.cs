using KeeperData.Core.EtlPipeline.Snapshots;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.EtlPipeline.Status;
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
        services.AddScoped<NormaliseStage>();
        services.AddScoped<SnapshotStage>();
        services.AddScoped<LoadDuckDbStage>();
        services.AddScoped<ExportSqliteStage>();

        
        services.AddXsvHcdtHelper();

        return services;
    }

    /// <summary>
    /// Records import status for every pipeline run. Separate from <see cref="AddEtlPipeline"/>
    /// because it needs Mongo and the pipeline itself does not: a host that only runs the pipeline
    /// (tests, tooling) should not have to provide a database to do it.
    /// </summary>
    public static IServiceCollection AddEtlImportStatus(this IServiceCollection services)
    {
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IEtlImportStatusStore, MongoEtlImportStatusStore>();

        // Status is derived from what the stages emit, so this observer is the only thing that knows
        // status exists; no stage reports its own progress.
        services.AddScoped<IPipelineRunObserver, EtlImportStatusObserver>();

        return services;
    }
}
