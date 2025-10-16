using KeeperData.Infrastructure.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KeeperData.Infrastructure.Extensions;

public static class TelemetryExtensions
{
    public static IServiceCollection AddKeeperDataMetrics(this IServiceCollection services)
    {
        services.TryAddSingleton<IApplicationMetrics, ApplicationMetrics>();
        services.TryAddSingleton<HealthCheckMetrics>();
        services.TryAddSingleton<HealthCheckMetricsPublisher>();
        
        return services;
    }
}