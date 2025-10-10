namespace KeeperData.Infrastructure.Metrics;

public interface IMetricsService
{
    void PutMetric(string metricName, double value, string unit = "Count", Dictionary<string, string>? dimensions = null);
    void PutMetric(string metricName, double value, Dictionary<string, string>? dimensions = null);
}