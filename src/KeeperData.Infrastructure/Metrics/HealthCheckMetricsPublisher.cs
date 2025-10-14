using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.Metrics;

[ExcludeFromCodeCoverage]
public class HealthCheckMetricsPublisher : IHealthCheckPublisher
{
    private readonly IHealthCheckMetrics _healthCheckMetrics;
    private readonly ILogger<HealthCheckMetricsPublisher> _logger;

    public HealthCheckMetricsPublisher(
        IHealthCheckMetrics healthCheckMetrics,
        ILogger<HealthCheckMetricsPublisher> logger)
    {
        _healthCheckMetrics = healthCheckMetrics;
        _logger = logger;
    }

    public async Task PublishAsync(HealthReport report, CancellationToken cancellationToken)
    {
        try
        {
            await _healthCheckMetrics.PublishAsync(report, cancellationToken);
            _logger.LogDebug("Health check metrics published successfully");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish health check metrics");
            // Don't rethrow - health check publishing should not fail the health check itself
        }
    }
}
