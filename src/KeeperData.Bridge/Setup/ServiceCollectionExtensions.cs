using KeeperData.Application.Setup;
using KeeperData.Infrastructure.Config;
using KeeperData.Infrastructure.Crypto;
using KeeperData.Bridge.Worker.Setup;
using KeeperData.Infrastructure.Database.Setup;
using KeeperData.Infrastructure.Messaging.Setup;
using KeeperData.Infrastructure.HealthCheck;
using KeeperData.Infrastructure.Metrics;
using KeeperData.Infrastructure.Storage.Setup;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;

namespace KeeperData.Bridge.Setup
{
    public static class ServiceCollectionExtensions
    {
        public static void ConfigureApi(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddDefaultAWSOptions(configuration.GetAWSOptions());
            services.Configure<AwsConfig>(configuration.GetSection(AwsConfig.SectionName));
            
            services.ConfigureHealthChecks();

            services.AddAWSService<Amazon.CloudWatch.IAmazonCloudWatch>();
            services.AddSingleton<IMetricsService, CloudWatchMetricsService>();
            services.AddSingleton<IApplicationMetrics, ApplicationMetrics>();
            services.AddHostedService<CloudWatchMeterListener>();

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
            
            // Register the health check publisher first  
            services.AddSingleton<HealthCheckMetricsPublisher>();
            services.AddSingleton<IHealthCheckPublisher>(serviceProvider => 
                serviceProvider.GetRequiredService<HealthCheckMetricsPublisher>());
            
            // Configure background health check publishing
            services.Configure<HealthCheckPublisherOptions>(options =>
            {
                options.Delay = TimeSpan.FromSeconds(10);    // Initial delay after startup
                options.Period = TimeSpan.FromSeconds(30);   // Repeat every 30 seconds
            });
            services.AddHostedService<HealthCheckPublisherHostedService>();
        }
    }
}