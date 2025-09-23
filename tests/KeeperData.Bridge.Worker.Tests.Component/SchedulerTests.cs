using KeeperData.Bridge.Worker.Jobs;
using KeeperData.Bridge.Worker.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Moq;
using Quartz;
using System.Threading.Tasks;

namespace KeeperData.Bridge.Worker.Tests.Component;

public class SchedulerTests
{
    [Fact]
    public async Task Scheduler_WhenJobIsTriggered_ExecutesOrchestratorJobSuccessfully()
    {
        var jobDidRun = new ManualResetEventSlim(false);

        var mockTaskA = new Mock<ITaskDownload>();
        var mockTaskB = new Mock<ITaskProcess>();

        mockTaskB.Setup(x => x.RunAsync(It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                jobDidRun.Set(); // Signal that the TaskB has completed
                return Task.CompletedTask;
            });

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices((hostContext, services) =>
            {
                services.AddScoped<ITaskDownload>(_ => mockTaskA.Object);
                services.AddScoped<ITaskProcess>(_ => mockTaskB.Object);

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
        mockTaskA.Verify(x => x.RunAsync(It.IsAny<CancellationToken>()), Times.Once);
        mockTaskB.Verify(x => x.RunAsync(It.IsAny<CancellationToken>()), Times.Once);
    }
}