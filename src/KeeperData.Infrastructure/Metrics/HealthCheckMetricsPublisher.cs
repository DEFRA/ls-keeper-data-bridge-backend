using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.Metrics;

[ExcludeFromCodeCoverage]
public class HealthCheckMetricsPublisher(IApplicationMetrics metrics, ILogger<HealthCheckMetricsPublisher> logger)
    : IHealthCheckPublisher
{
    public Task PublishAsync(HealthReport report, CancellationToken cancellationToken)
    {
        try
        {
            var overallStatus = report.Status.ToString();
            var totalDurationMs = report.TotalDuration.TotalMilliseconds;

            metrics.RecordRequest("health_check", overallStatus.ToLowerInvariant());
            metrics.RecordDuration("health_check", totalDurationMs);

            logger.LogDebug("Recorded overall health check metrics: Status={Status}, Duration={Duration}ms", 
                overallStatus, totalDurationMs);
            
            foreach (var entry in report.Entries)
            {
                var checkName = entry.Key;
                var checkStatus = entry.Value.Status.ToString();
                var checkDurationMs = entry.Value.Duration.TotalMilliseconds;

                metrics.RecordCount("health_check_individual", 1, 
                    ("check_name", checkName), 
                    ("status", checkStatus.ToLowerInvariant()));
                
                metrics.RecordValue("health_check_individual_duration", checkDurationMs,
                    ("check_name", checkName), 
                    ("status", checkStatus.ToLowerInvariant()));

                metrics.RecordCount("health_check_executions", 1, 
                    ("check_name", checkName), 
                    ("status", checkStatus.ToLowerInvariant()),
                    ("overall_status", overallStatus.ToLowerInvariant()));

                metrics.RecordValue("health_check_duration_ms", checkDurationMs,
                    ("check_name", checkName), 
                    ("status", checkStatus.ToLowerInvariant()));

                logger.LogDebug("Recorded health check metrics for {CheckName}: Status={Status}, Duration={Duration}ms", 
                    checkName, checkStatus, checkDurationMs);

                if (entry.Value.Data.Count > 0)
                {
                    foreach (var data in entry.Value.Data)
                    {
                        logger.LogDebug("Health check {CheckName} data: {Key}={Value}", 
                            checkName, data.Key, data.Value);
                    }
                }

                if (entry.Value.Exception != null)
                {
                    logger.LogWarning("Health check {CheckName} failed with exception: {Exception}", 
                        checkName, entry.Value.Exception.Message);
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to publish health check metrics");
            metrics.RecordRequest("health_check_metrics_publisher", "error");
        }

        return Task.CompletedTask;
    }
}