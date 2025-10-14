using Amazon.CloudWatch.EMF.Logger;
using Amazon.CloudWatch.EMF.Model;
using KeeperData.Infrastructure.Config;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.Metrics;

[ExcludeFromCodeCoverage]
public class EmfHealthCheckMetrics : IHealthCheckMetrics
{
    private readonly ILogger<EmfHealthCheckMetrics> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly string _namespace;
    private readonly string _serviceName;

    public EmfHealthCheckMetrics(
        ILogger<EmfHealthCheckMetrics> logger,
        ILoggerFactory loggerFactory,
        IOptions<AwsConfig> awsConfig)
    {
        _logger = logger;
        _loggerFactory = loggerFactory;
        _namespace = awsConfig.Value.EMF.Namespace;
        _serviceName = awsConfig.Value.EMF.ServiceName;
    }

    public Task PublishAsync(HealthReport report, CancellationToken cancellationToken = default)
    {
        try
        {
            using var metricsLogger = new MetricsLogger(_loggerFactory);
            
            metricsLogger.SetNamespace(_namespace);
            
            // Set service dimension
            var dimensionSet = new DimensionSet();
            dimensionSet.AddDimension("service", _serviceName);
            metricsLogger.SetDimensions(dimensionSet);

            // Record overall health status
            var overallStatusValue = report.Status == HealthStatus.Healthy ? 1.0 : 0.0;
            metricsLogger.PutMetric("health_check_status", overallStatusValue, Unit.COUNT);

            // Record total duration
            metricsLogger.PutMetric("health_check_duration_ms", report.TotalDuration.TotalMilliseconds, Unit.MILLISECONDS);

            // Record individual check results
            foreach (var entry in report.Entries)
            {
                var checkDimensionSet = new DimensionSet();
                checkDimensionSet.AddDimension("service", _serviceName);
                checkDimensionSet.AddDimension("check_name", entry.Key);
                
                metricsLogger.SetDimensions(checkDimensionSet);
                
                var checkStatusValue = entry.Value.Status == HealthStatus.Healthy ? 1.0 : 0.0;
                metricsLogger.PutMetric("health_check_individual_status", checkStatusValue, Unit.COUNT);
                metricsLogger.PutMetric("health_check_individual_duration_ms", entry.Value.Duration.TotalMilliseconds, Unit.MILLISECONDS);
            }

            metricsLogger.Flush();

            _logger.LogDebug("Successfully published health check metrics via EMF for {ServiceName}", _serviceName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish health check metrics via EMF");
            throw;
        }

        return Task.CompletedTask;
    }
}
