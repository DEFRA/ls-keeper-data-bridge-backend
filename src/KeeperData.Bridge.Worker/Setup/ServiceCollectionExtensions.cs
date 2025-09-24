using KeeperData.Bridge.Worker.Configuration;
using KeeperData.Bridge.Worker.Jobs;
using KeeperData.Bridge.Worker.Tasks;
using KeeperData.Bridge.Worker.Tasks.Implementations;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Quartz;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Bridge.Worker.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    public static void AddBackgroundJobDependencies(this IServiceCollection services, IConfiguration configuration)
    {
        services
            .AddQuartz(configuration)
            .AddJobs()
            .AddTasks();
    }

    private static IServiceCollection AddQuartz(this IServiceCollection services, IConfiguration configuration)
    {
        var scheduledJobConfiguration = configuration.GetRequiredSection("Quartz:Jobs").Get<List<ScheduledJobConfiguration>>() ?? [];

        services.AddQuartz(q =>
        {
            var importBulkFilesConfig = scheduledJobConfiguration.FirstOrDefault(x => x.JobType == nameof(ImportBulkFilesJob));
            if (importBulkFilesConfig?.CronSchedule != null)
            {
                q.AddJob<ImportBulkFilesJob>(opts => opts.WithIdentity(importBulkFilesConfig.JobType));

                q.AddTrigger(opts => opts
                    .ForJob(importBulkFilesConfig.JobType)
                    .WithIdentity($"{importBulkFilesConfig.JobType}-trigger")
                    .WithCronSchedule(importBulkFilesConfig.CronSchedule));
            }

            var importDeltaFilesConfig = scheduledJobConfiguration.FirstOrDefault(x => x.JobType == nameof(ImportDeltaFilesJob));
            if (importDeltaFilesConfig?.CronSchedule != null)
            {
                q.AddJob<ImportDeltaFilesJob>(opts => opts.WithIdentity(importDeltaFilesConfig.JobType));

                q.AddTrigger(opts => opts
                    .ForJob(importDeltaFilesConfig.JobType)
                    .WithIdentity($"{importDeltaFilesConfig.JobType}-trigger")
                    .WithCronSchedule(importDeltaFilesConfig.CronSchedule));
            }
        });

        services.AddQuartzHostedService(q =>
        {
            q.WaitForJobsToComplete = false;
        });

        return services;
    }

    private static IServiceCollection AddJobs(this IServiceCollection services)
    {
        services.AddScoped<ImportBulkFilesJob>();
        services.AddScoped<ImportDeltaFilesJob>();

        return services;
    }

    private static IServiceCollection AddTasks(this IServiceCollection services)
    {
        services.AddScoped<ITaskProcessBulkFiles, TaskProcessBulkFiles>();
        services.AddScoped<ITaskProcessDeltaFiles, TaskProcessDeltaFiles>();

        return services;
    }
}