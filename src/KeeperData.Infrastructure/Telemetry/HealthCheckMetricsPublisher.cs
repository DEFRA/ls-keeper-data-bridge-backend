using System.Diagnostics;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace KeeperData.Infrastructure.Telemetry;

public class HealthCheckMetricsPublisher : IHealthCheckPublisher
{
    private readonly HealthCheckMetrics _metrics;
    private readonly ILogger<HealthCheckMetricsPublisher> _logger;

    public HealthCheckMetricsPublisher(HealthCheckMetrics metrics, ILogger<HealthCheckMetricsPublisher> logger)
    {
        _metrics = metrics;
        _logger = logger;
    }

    public Task PublishAsync(HealthReport report, CancellationToken cancellationToken)
    {
        try
        {
            foreach (var entry in report.Entries)
            {
                var healthCheckName = entry.Key;
                var result = entry.Value;
                var durationMs = result.Duration.TotalMilliseconds;

                _metrics.RecordHealthCheck(healthCheckName, result.Status, durationMs);

                _logger.LogDebug("Published health check metric for {HealthCheck} with status {Status} in {Duration}ms", 
                    healthCheckName, result.Status, durationMs);
            }

            // Record overall health status
            _metrics.RecordHealthCheck("overall", report.Status, report.TotalDuration.TotalMilliseconds);
        }
        catch (Exception ex)
        {
            // Don't let metrics publishing fail health checks
            _logger.LogError(ex, "Failed to publish health check metrics");
        }

        return Task.CompletedTask;
    }
}