using KeeperData.Bridge.Worker.Jobs;
using KeeperData.Bridge.Worker.Tasks;
using Quartz;
using Serilog;

var host = Host.CreateDefaultBuilder(args)
    .UseSerilog((context, config) => config.ReadFrom.Configuration(context.Configuration))
    .ConfigureServices((hostContext, services) =>
    {
        services.AddScoped<ITaskDownload, TaskDownload>();
        services.AddScoped<ITaskProcess, TaskProcess>();
        services.AddScoped<DataProcessingOrchestratorJob>();

        services.AddQuartz(q =>
        {
            var jobKey = new JobKey("DataProcessingOrchestratorJob");
            var cronSchedule = hostContext.Configuration["Quartz:Jobs:DataProcessingOrchestrator:CronSchedule"];

            if (string.IsNullOrWhiteSpace(cronSchedule))
            {
                throw new ApplicationException("Cron schedule for the orchestrator job is not configured.");
            }

            q.AddJob<DataProcessingOrchestratorJob>(opts => opts.WithIdentity(jobKey));

            // trigger that runs on the schedule from appsettings.json
            q.AddTrigger(opts => opts
                .ForJob(jobKey)
                .WithIdentity($"{jobKey.Name}-trigger")
                .WithCronSchedule(cronSchedule));
        });

        services.AddQuartzHostedService(q => q.WaitForJobsToComplete = true);
    })
    .Build();

await host.RunAsync();