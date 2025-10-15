using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace KeeperData.Infrastructure.Telemetry;

public class HealthCheckMetrics
{
    private readonly Counter<long> _healthCheckTotal;
    private readonly Counter<long> _healthCheckFailures;
    private readonly Histogram<double> _healthCheckDuration;
    private readonly UpDownCounter<int> _healthCheckStatus;

    public HealthCheckMetrics(IMeterFactory meterFactory)
    {
        var meter = meterFactory.Create(MetricNames.MeterName);
        
        _healthCheckTotal = meter.CreateCounter<long>(
            "keeperdata.health.checks.total", 
            "ea", 
            "Total number of health checks performed");
            
        _healthCheckFailures = meter.CreateCounter<long>(
            "keeperdata.health.checks.failures", 
            "ea", 
            "Number of health check failures");
            
        _healthCheckDuration = meter.CreateHistogram<double>(
            "keeperdata.health.checks.duration", 
            "ms", 
            "Health check execution duration in milliseconds");
            
        _healthCheckStatus = meter.CreateUpDownCounter<int>(
            "keeperdata.health.status", 
            "status", 
            "Current health check status (2=Healthy, 1=Degraded, 0=Unhealthy)");
    }

    public void RecordHealthCheck(string healthCheckName, HealthStatus status, double durationMs)
    {
        var tagList = new TagList
        {
            { MetricNames.CommonTags.Service, Process.GetCurrentProcess().ProcessName },
            { MetricNames.CommonTags.HealthCheck, healthCheckName },
            { MetricNames.CommonTags.Status, status.ToString() }
        };

        _healthCheckTotal.Add(1, tagList);
        _healthCheckDuration.Record(durationMs, tagList);

        if (status != HealthStatus.Healthy)
        {
            _healthCheckFailures.Add(1, tagList);
        }

        // Record current health status as numeric value
        var statusValue = status switch 
        {
            HealthStatus.Healthy => 2,
            HealthStatus.Degraded => 1,
            HealthStatus.Unhealthy => 0,
            _ => -1 // Unknown status
        };
        _healthCheckStatus.Add(statusValue, tagList);
    }
}