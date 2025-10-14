using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace KeeperData.Infrastructure.Metrics;

/// <summary>
/// Simple interface for publishing health check metrics only
/// </summary>
public interface IHealthCheckMetrics
{
    /// <summary>
    /// Publishes health check results as metrics
    /// </summary>
    /// <param name="report">The health check report containing results</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task PublishAsync(HealthReport report, CancellationToken cancellationToken = default);
}
