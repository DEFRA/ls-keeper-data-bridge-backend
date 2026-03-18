using FluentAssertions;
using KeeperData.Bridge.Setup;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace KeeperData.Bridge.Tests.Component.Setup;

public class QuartzJobsHealthCheckTests
{
    private readonly HealthCheckContext _healthCheckContext = new();

    [Fact]
    public async Task CheckHealthAsync_WithEnabledAndDisabledJobs_ShouldReturnAllJobsWithStatus()
    {
        var config = BuildConfiguration(new Dictionary<string, string?>
        {
            ["Quartz:Jobs:0:JobType"] = "ImportBulkFilesJob",
            ["Quartz:Jobs:0:IsEnabled"] = "true",
            ["Quartz:Jobs:0:CronSchedule"] = "0 0 8 * * ?",
            ["Quartz:Jobs:1:JobType"] = "CleanseReportJob",
            ["Quartz:Jobs:1:IsEnabled"] = "false",
            ["Quartz:Jobs:1:CronSchedule"] = "0 0 2 * * ?",
        });

        var sut = new QuartzJobsHealthCheck(config);
        var result = await sut.CheckHealthAsync(_healthCheckContext, CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Healthy);
        result.Description.Should().Be("Quartz scheduled jobs");
        result.Data.Should().HaveCount(2);
        result.Data.Should().ContainKey("ImportBulkFilesJob");
        result.Data.Should().ContainKey("CleanseReportJob");
    }

    [Fact]
    public async Task CheckHealthAsync_WithNoJobsConfigured_ShouldReturnHealthyWithEmptyData()
    {
        var config = BuildConfiguration([]);

        var sut = new QuartzJobsHealthCheck(config);
        var result = await sut.CheckHealthAsync(_healthCheckContext, CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Healthy);
        result.Description.Should().Be("Quartz scheduled jobs");
        result.Data.Should().BeEmpty();
    }

    [Fact]
    public async Task CheckHealthAsync_ShouldIncludeIsEnabledAndCronScheduleInData()
    {
        var config = BuildConfiguration(new Dictionary<string, string?>
        {
            ["Quartz:Jobs:0:JobType"] = "TestJob",
            ["Quartz:Jobs:0:IsEnabled"] = "true",
            ["Quartz:Jobs:0:CronSchedule"] = "0 30 6 * * ?",
        });

        var sut = new QuartzJobsHealthCheck(config);
        var result = await sut.CheckHealthAsync(_healthCheckContext, CancellationToken.None);

        result.Data.Should().ContainKey("TestJob");
        var jobData = result.Data["TestJob"];
        var isEnabled = jobData.GetType().GetProperty("IsEnabled")!.GetValue(jobData);
        var cronSchedule = jobData.GetType().GetProperty("CronSchedule")!.GetValue(jobData);
        isEnabled.Should().Be(true);
        cronSchedule.Should().Be("0 30 6 * * ?");
    }

    [Fact]
    public async Task CheckHealthAsync_WithDisabledJob_ShouldReportIsEnabledFalse()
    {
        var config = BuildConfiguration(new Dictionary<string, string?>
        {
            ["Quartz:Jobs:0:JobType"] = "DisabledJob",
            ["Quartz:Jobs:0:IsEnabled"] = "false",
            ["Quartz:Jobs:0:CronSchedule"] = "0 0 0 * * ?",
        });

        var sut = new QuartzJobsHealthCheck(config);
        var result = await sut.CheckHealthAsync(_healthCheckContext, CancellationToken.None);

        result.Data.Should().ContainKey("DisabledJob");
        var jobData = result.Data["DisabledJob"];
        var isEnabled = jobData.GetType().GetProperty("IsEnabled")!.GetValue(jobData);
        isEnabled.Should().Be(false);
    }

    private static IConfiguration BuildConfiguration(Dictionary<string, string?> values)
    {
        return new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();
    }
}
