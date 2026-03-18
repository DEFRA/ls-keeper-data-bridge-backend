using KeeperData.Bridge.Worker.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace KeeperData.Bridge.Setup;

public class QuartzJobsHealthCheck(IConfiguration configuration) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var jobs = configuration.GetSection("Quartz:Jobs").Get<List<ScheduledJobConfiguration>>() ?? [];

        var data = new Dictionary<string, object>();
        foreach (var job in jobs)
        {
            data[job.JobType] = new
            {
                job.IsEnabled,
                job.CronSchedule
            };
        }

        return Task.FromResult(HealthCheckResult.Healthy("Quartz scheduled jobs", data));
    }
}
