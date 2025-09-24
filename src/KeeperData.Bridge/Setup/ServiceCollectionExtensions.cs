using KeeperData.Application.Setup;
using KeeperData.Bridge.Worker.Jobs;
using KeeperData.Bridge.Worker.Tasks;
using KeeperData.Infrastructure.Database.Setup;
using KeeperData.Infrastructure.Messaging.Setup;
using KeeperData.Infrastructure.Storage.Setup;
using Quartz;

namespace KeeperData.Bridge.Setup
{
    public static class ServiceCollectionExtensions
    {
        public static void ConfigureApi(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddDefaultAWSOptions(configuration.GetAWSOptions());

            services.ConfigureHealthChecks();

            services.AddApplicationLayer();

            services.AddDatabaseDependencies(configuration);

            services.AddMessagingDependencies(configuration);

            services.AddStorageDependencies(configuration);

            services.AddJobServices();
            services.AddConfiguredQuartz(configuration);
        }

        private static void ConfigureHealthChecks(this IServiceCollection services)
        {
            services.AddHealthChecks();
        }

        private static void AddJobServices(this IServiceCollection services)
        {
            services.AddScoped<ITaskDownload, TaskDownload>();
            services.AddScoped<ITaskProcess, TaskProcess>();
            services.AddScoped<DataProcessingOrchestratorJob>();
        }

        private static void AddConfiguredQuartz(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddQuartz(q =>
            {
                var jobKey = new JobKey("DataProcessingOrchestratorJob");
                var cronSchedule = configuration["Quartz:Jobs:DataProcessingOrchestrator:CronSchedule"];

                if (string.IsNullOrWhiteSpace(cronSchedule))
                {
                    Console.WriteLine("WARNING: Cron schedule for the orchestrator job is not configured. The job will not run.");
                    return;
                }

                q.AddJob<DataProcessingOrchestratorJob>(opts => opts.WithIdentity(jobKey));

                q.AddTrigger(opts => opts
                    .ForJob(jobKey)
                    .WithIdentity($"{jobKey.Name}-trigger")
                    .WithCronSchedule(cronSchedule));
            });

            services.AddQuartzHostedService(q =>
            {
                q.WaitForJobsToComplete = false;
            });
        }
    }
}