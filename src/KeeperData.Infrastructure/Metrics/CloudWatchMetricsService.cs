using Amazon.CloudWatch;
using Amazon.CloudWatch.Model;
using KeeperData.Infrastructure.Config;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.Metrics;

[ExcludeFromCodeCoverage]
public class CloudWatchMetricsService : IMetricsService
{
    private readonly IAmazonCloudWatch _cloudWatch;
    private readonly ILogger<CloudWatchMetricsService> _logger;
    private readonly AwsConfig _config;

    public CloudWatchMetricsService(
        IAmazonCloudWatch cloudWatch,
        ILogger<CloudWatchMetricsService> logger,
        IOptions<AwsConfig> config)
    {
        _cloudWatch = cloudWatch;
        _logger = logger;
        _config = config.Value;
    }

    public void PutMetric(string metricName, double value, string unit = "Count", Dictionary<string, string>? dimensions = null)
    {
        var standardUnit = ParseUnit(unit);
        var dimensionList = dimensions?.Select(d => new Dimension { Name = d.Key, Value = d.Value }).ToList() ?? new List<Dimension>();
        
        _ = Task.Run(async () => await PutMetricDataAsync(metricName, value, standardUnit, dimensionList));
    }

    public void PutMetric(string metricName, double value, Dictionary<string, string>? dimensions = null)
    {
        PutMetric(metricName, value, "Count", dimensions);
    }

    public async Task RecordCountAsync(string metricName, double count, params (string Key, string Value)[] dimensions)
    {
        var dimensionList = dimensions.Select(d => new Dimension { Name = d.Key, Value = d.Value }).ToList();
        await PutMetricDataAsync(metricName, count, StandardUnit.Count, dimensionList);
    }

    public async Task RecordValueAsync(string metricName, double value, params (string Key, string Value)[] dimensions)
    {
        var dimensionList = dimensions.Select(d => new Dimension { Name = d.Key, Value = d.Value }).ToList();
        await PutMetricDataAsync(metricName, value, StandardUnit.None, dimensionList);
    }

    public async Task RecordDurationAsync(string metricName, double milliseconds, params (string Key, string Value)[] dimensions)
    {
        var dimensionList = dimensions.Select(d => new Dimension { Name = d.Key, Value = d.Value }).ToList();
        await PutMetricDataAsync(metricName, milliseconds, StandardUnit.Milliseconds, dimensionList);
    }

    private StandardUnit ParseUnit(string unit)
    {
        return unit.ToLowerInvariant() switch
        {
            "count" => StandardUnit.Count,
            "milliseconds" => StandardUnit.Milliseconds,
            "seconds" => StandardUnit.Seconds,
            "percent" => StandardUnit.Percent,
            _ => StandardUnit.None
        };
    }

    private async Task PutMetricDataAsync(string metricName, double value, StandardUnit unit, List<Dimension> dimensions)
    {
        try
        {
            var metricDatum = new MetricDatum
            {
                MetricName = metricName,
                Value = value,
                Unit = unit,
                Timestamp = DateTime.UtcNow,
                Dimensions = dimensions
            };

            var request = new PutMetricDataRequest
            {
                Namespace = GetNamespace(),
                MetricData = new List<MetricDatum> { metricDatum }
            };

            await _cloudWatch.PutMetricDataAsync(request);

            _logger.LogDebug("Successfully published metric {MetricName} with value {Value} and {DimensionCount} dimensions to CloudWatch namespace {Namespace}",
                metricName, value, dimensions.Count, GetNamespace());
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish metric {MetricName} to CloudWatch", metricName);
            throw;
        }
    }

    private string GetNamespace()
    {
        return _config.EMF.Namespace;
    }
}