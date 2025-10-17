using System.Diagnostics;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace KeeperData.Infrastructure.Telemetry;

public class HealthCheckMetricsPublisher : IHealthCheckPublisher
{
    private readonly HealthCheckMetrics _healthMetrics;
    private readonly IApplicationMetrics _applicationMetrics;
    private readonly ILogger<HealthCheckMetricsPublisher> _logger;

    public HealthCheckMetricsPublisher(
        HealthCheckMetrics healthMetrics,
        IApplicationMetrics applicationMetrics,
        ILogger<HealthCheckMetricsPublisher> logger)
    {
        _healthMetrics = healthMetrics;
        _applicationMetrics = applicationMetrics;
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

                // Use specialized health check metrics for detailed tracking
                _healthMetrics.RecordHealthCheck(healthCheckName, result.Status, durationMs);

                // Use generic application metrics for broader monitoring
                _applicationMetrics.RecordRequest("health_check", result.Status.ToString().ToLowerInvariant());
                _applicationMetrics.RecordDuration("health_check", durationMs);

                _logger.LogDebug("Published health check metric for {HealthCheck} with status {Status} in {Duration}ms",
                    healthCheckName, result.Status, durationMs);
            }

            // Record overall health status
            _healthMetrics.RecordHealthCheck("overall", report.Status, report.TotalDuration.TotalMilliseconds);

            // Record summary metrics using IApplicationMetrics
            _applicationMetrics.RecordRequest("health_check_overall", report.Status.ToString().ToLowerInvariant());
            _applicationMetrics.RecordDuration("health_check_overall", report.TotalDuration.TotalMilliseconds);
            _applicationMetrics.RecordCount("health_checks_executed", report.Entries.Count);
        }
        catch (Exception ex)
        {
            // Don't let metrics publishing fail health checks
            _logger.LogError(ex, "Failed to publish health check metrics");

            // Record error metric
            _applicationMetrics.RecordRequest("health_check_publisher", "error");
        }

        return Task.CompletedTask;
    }
}