using System.Text.Json;
using KeeperData.Infrastructure.Config;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KeeperData.Infrastructure.Metrics;

public class EmfMetricsService(ILogger<EmfMetricsService> logger, IOptions<AwsConfig> awsConfig)
    : IMetricsService
{
    private readonly string _namespace = awsConfig.Value.EMF.Namespace;
    private readonly string _serviceName = awsConfig.Value.EMF.ServiceName;

    public void PutMetric(string metricName, double value, string unit = "Count", Dictionary<string, string>? dimensions = null)
    {
        var emfLog = CreateEmfLog(metricName, value, unit, dimensions);
        logger.LogInformation("{EmfMetric}", emfLog);
    }

    public void PutMetric(string metricName, double value, Dictionary<string, string>? dimensions = null)
    {
        PutMetric(metricName, value, "Count", dimensions);
    }

    private string CreateEmfLog(string metricName, double value, string unit, Dictionary<string, string>? dimensions)
    {
        var logData = new Dictionary<string, object>
        {
            ["_aws"] = new
            {
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                CloudWatchMetrics = new[]
                {
                    new
                    {
                        Namespace = _namespace,
                        Dimensions = new[] { new[] { "ServiceName" } },
                        Metrics = new[]
                        {
                            new { Name = metricName, Unit = unit }
                        }
                    }
                }
            },
            ["ServiceName"] = _serviceName,
            [metricName] = value
        };

        if (dimensions != null)
        {
            foreach (var dimension in dimensions)
            {
                logData[dimension.Key] = dimension.Value;
            }
        }

        return JsonSerializer.Serialize(logData);
    }
}