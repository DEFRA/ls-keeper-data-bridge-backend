using KeeperData.Infrastructure.Benchmarking.Services;
using KeeperData.Infrastructure.Benchmarking.Throttling;
using KeeperData.Infrastructure.Database.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace KeeperData.Infrastructure.Benchmarking.Setup;

public static class BenchmarkServiceCollectionExtensions
{
    /// <summary>
    /// Registers the benchmark orchestrator as a singleton.
    /// It creates its own instrumented <see cref="MongoClient"/> per run so
    /// it does not interfere with the application's existing client.
    /// The <see cref="DefaultBenchmarkThrottler"/> is used to throttle
    /// operations in production; tests can register <see cref="NoOpBenchmarkThrottler"/>
    /// instead.
    /// </summary>
    public static IServiceCollection AddBenchmarkServices(this IServiceCollection services)
    {
        // Register the default (production) throttler only if one has not
        // already been registered — allows tests to pre-register NoOpBenchmarkThrottler.
        services.TryAddSingleton<IBenchmarkThrottler, DefaultBenchmarkThrottler>();

        services.AddSingleton<IBenchmarkOrchestrator>(sp =>
        {
            var mongoConfig = sp.GetRequiredService<IOptions<MongoConfig>>().Value;
            var settings = MongoClientSettings.FromConnectionString(mongoConfig.DatabaseUri);
            // Preserve database name so the orchestrator can resolve it
            settings.ApplicationName = mongoConfig.DatabaseName;

            var throttler = sp.GetRequiredService<IBenchmarkThrottler>();
            var logger = sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<BenchmarkOrchestrator>>();
            return new BenchmarkOrchestrator(settings, throttler, logger);
        });

        return services;
    }
}
