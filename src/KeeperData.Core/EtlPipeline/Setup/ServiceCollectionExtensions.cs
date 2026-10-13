using KeeperData.Core.EtlPipeline.Concurrency;
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

        // The budget is a property of the host, not of a run: one instance so the limit still holds
        // when several stages are fanning out at once.
        services.AddOptions<EtlConcurrencyOptions>();
        services.TryAddSingleton<EtlConcurrency>();

        // Unbound here so a host that never configures them still resolves them, off.
        services.AddOptions<EtlFeatureFlags>();

        services.AddScoped<IPipelineExecutor, PipelineExecutor>();
        services.AddScoped<IEtlPipelineFactory, EtlPipelineFactory>();

        services.TryAddScoped<IDeltaMergeEngine, ParquetDeltaMergeEngine>();

        services.AddScoped<DecryptStage>();
        services.AddScoped<NormaliseStage>();
        services.AddScoped<OptimiseStage>();
        services.AddScoped<SnapshotStage>();
        services.AddScoped<LoadDuckDbStage>();
        services.AddScoped<ExportSqliteStage>();
        services.AddScoped<EtlStages>();

        services.AddXsvHcdtHelper(x =>
        {
            x.InputQuoting = QuoteHandling.None;
            x.ValidateHeaderTrailerMatch = false;
            x.ValidateTrailerCount = false;
            x.RequireTrailer = false;
            x.OutputFormat = OutputFormat.Parquet;
            x.StrictFieldCount = false;
        });

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
