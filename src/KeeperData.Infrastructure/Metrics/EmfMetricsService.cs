using Amazon.CloudWatch.EMF.Logger;
using Amazon.CloudWatch.EMF.Model;
using KeeperData.Infrastructure.Config;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Unit = Amazon.CloudWatch.EMF.Model.Unit;

namespace KeeperData.Infrastructure.Metrics;

public class EmfMetricsService(ILogger<EmfMetricsService> logger, IOptions<AwsConfig> awsConfig, ILoggerFactory loggerFactory)
    : IMetricsService
{
    private readonly string _namespace = awsConfig.Value.EMF.Namespace;
    private readonly string _serviceName = awsConfig.Value.EMF.ServiceName;
    private readonly ILoggerFactory _loggerFactory = loggerFactory;

    public void PutMetric(string metricName, double value, string unit = "Count", Dictionary<string, string>? dimensions = null)
    {
        try
        {
            using var metricsLogger = new MetricsLogger(_loggerFactory);
            
            metricsLogger.SetNamespace(_namespace);
            
            var dimensionSet = new DimensionSet();
            dimensionSet.AddDimension("ServiceName", _serviceName);
            
            if (dimensions != null)
            {
                foreach (var dimension in dimensions)
                {
                    if (!string.IsNullOrWhiteSpace(dimension.Value))
                    {
                        dimensionSet.AddDimension(dimension.Key, dimension.Value);
                    }
                }
            }
            
            metricsLogger.SetDimensions(dimensionSet);
            
            var emfUnit = ParseUnit(unit);
            metricsLogger.PutMetric(metricName, value, emfUnit);
            metricsLogger.Flush();
            
            var dimensionCount = dimensions?.Count ?? 0;
            logger.LogDebug("EMF metric recorded: {MetricName}={Value} {Unit} with {DimensionCount} dimensions", 
                metricName, value, unit, dimensionCount + 1); // +1 for ServiceName
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to record EMF metric {MetricName}", metricName);
        }
    }

    public void PutMetric(string metricName, double value, Dictionary<string, string>? dimensions = null)
    {
        PutMetric(metricName, value, "Count", dimensions);
    }

    private static Unit ParseUnit(string unit)
    {
        return unit.ToLowerInvariant() switch
        {
            "count" => Unit.COUNT,
            "milliseconds" => Unit.MILLISECONDS,
            "seconds" => Unit.SECONDS,
            "bytes" => Unit.BYTES,
            "kilobytes" => Unit.KILOBYTES,
            "megabytes" => Unit.MEGABYTES,
            "gigabytes" => Unit.GIGABYTES,
            "percent" => Unit.PERCENT,
            "none" => Unit.NONE,
            _ => Enum.TryParse<Unit>(unit, true, out var parsedUnit) ? parsedUnit : Unit.COUNT
        };
    }
}