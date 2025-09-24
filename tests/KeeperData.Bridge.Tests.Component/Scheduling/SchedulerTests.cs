using KeeperData.Bridge.Worker.Jobs;
using KeeperData.Bridge.Worker.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Moq;
using Quartz;

namespace KeeperData.Bridge.Tests.Component;

public class SchedulerTests
{
    [Fact]
    public async Task Scheduler_WhenJobIsTriggered_ExecutesOrchestratorJobSuccessfully()
    {
        var jobDidRun = new ManualResetEventSlim(false);

        var mockTaskDownload = new Mock<ITaskDownload>();
        var mockTaskProcess = new Mock<ITaskProcess>();

        mockTaskProcess.Setup(x => x.RunAsync(It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                jobDidRun.Set();
                return Task.CompletedTask;
            });

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices((hostContext, services) =>
            {
                services.AddScoped<ITaskDownload>(_ => mockTaskDownload.Object);
                services.AddScoped<ITaskProcess>(_ => mockTaskProcess.Object);

                services.AddScoped<DataProcessingOrchestratorJob>();

                services.AddQuartz(q =>
                {

                    q.UseInMemoryStore();

                    var jobKey = new JobKey("TestOrchestratorJob");

                    // Durable as don't want a timed trigger in tests
                    q.AddJob<DataProcessingOrchestratorJob>(opts => opts.WithIdentity(jobKey).StoreDurably());
                });
                services.AddQuartzHostedService(q => q.WaitForJobsToComplete = true);
            }).Build();


        await host.StartAsync();

        var scheduler = host.Services.GetRequiredService<ISchedulerFactory>().GetScheduler().Result;

        await scheduler.TriggerJob(new JobKey("TestOrchestratorJob"));

        var completedInTime = jobDidRun.Wait(TimeSpan.FromSeconds(10));

        await host.StopAsync();

        Assert.True(completedInTime, "The job did not complete in the expected time.");
        mockTaskDownload.Verify(x => x.RunAsync(It.IsAny<CancellationToken>()), Times.Once);
        mockTaskProcess.Verify(x => x.RunAsync(It.IsAny<CancellationToken>()), Times.Once);
    }
}