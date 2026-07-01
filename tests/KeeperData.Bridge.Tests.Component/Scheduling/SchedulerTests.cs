using KeeperData.Bridge.Worker.Coordination;
using KeeperData.Bridge.Worker.Jobs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Moq;
using Quartz;

namespace KeeperData.Bridge.Tests.Component.Scheduling;

public class SchedulerTests
{
    [Fact]
    public async Task Scheduler_WhenJobIsTriggered_ExecutesOrchestratorJobSuccessfully()
    {
        var jobDidRun = new ManualResetEventSlim(false);

        var coordinatorMock = new Mock<IIngestionRunCoordinator>();

        coordinatorMock.Setup(x => x.RunAsync(It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                jobDidRun.Set();
                return Task.CompletedTask;
            });

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices((hostContext, services) =>
            {
                services.AddScoped(_ => coordinatorMock.Object);
                services.AddScoped<ImportBulkFilesJob>();

                services.AddQuartz(q =>
                {
                    q.UseInMemoryStore();

                    // Durable as don't want a timed trigger in tests
                    var jobKey = new JobKey("TestJob");
                    q.AddJob<ImportBulkFilesJob>(opts => opts.WithIdentity(jobKey).StoreDurably());
                });
                services.AddQuartzHostedService(q => q.WaitForJobsToComplete = true);
            }).Build();

        await host.StartAsync();

        var scheduler = await host.Services.GetRequiredService<ISchedulerFactory>().GetScheduler();

        await scheduler.TriggerJob(new JobKey("TestJob"));

        var completedInTime = jobDidRun.Wait(TimeSpan.FromSeconds(10));

        await host.StopAsync();

        Assert.True(completedInTime, "The job did not complete in the expected time.");
        coordinatorMock.Verify(x => x.RunAsync(It.IsAny<CancellationToken>()), Times.Once);
    }
}
