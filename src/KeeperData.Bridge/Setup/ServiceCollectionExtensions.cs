using KeeperData.Application.Setup;
using KeeperData.Infrastructure.Config;
using KeeperData.Infrastructure.Crypto;
using KeeperData.Bridge.Worker.Setup;
using KeeperData.Infrastructure.Database.Setup;
using KeeperData.Infrastructure.Messaging.Setup;
using KeeperData.Infrastructure.Storage.Setup;
using KeeperData.Infrastructure.Metrics;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace KeeperData.Bridge.Setup
{
    public static class ServiceCollectionExtensions
    {
        public static void ConfigureApi(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddDefaultAWSOptions(configuration.GetAWSOptions());
            services.Configure<AwsConfig>(configuration.GetSection(AwsConfig.SectionName));
            
            services.ConfigureHealthChecks();

            services.AddApplicationLayer();

            services.AddDatabaseDependencies(configuration);

            services.AddMessagingDependencies(configuration);

            services.AddStorageDependencies(configuration);

            services.AddCrypto(configuration);

            services.AddBackgroundJobDependencies(configuration);
        }

        private static void ConfigureHealthChecks(this IServiceCollection services)
        {
            services.AddHealthChecks();
            
            // Register EMF health check metrics
            services.AddSingleton<IHealthCheckMetrics, EmfHealthCheckMetrics>();
            services.AddSingleton<HealthCheckMetricsPublisher>();
            services.AddSingleton<IHealthCheckPublisher>(serviceProvider => 
                serviceProvider.GetRequiredService<HealthCheckMetricsPublisher>());
        }
    }
}