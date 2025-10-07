using System.Diagnostics.Metrics;
using KeeperData.Infrastructure.Config;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace KeeperData.Infrastructure.Metrics;

public interface IApplicationMetrics
{
    void RecordRequest(string operation, string status);
    void RecordDuration(string operation, double durationMs);
    void RecordCount(string metricName, long count = 1, params (string Key, string Value)[] tags);
    void RecordValue(string metricName, double value, params (string Key, string Value)[] tags);
}

public class ApplicationMetrics : IApplicationMetrics, IDisposable
{
    private readonly Meter _meter;
    private readonly Counter<long> _requestCounter;
    private readonly Histogram<double> _requestDuration;
    private readonly Counter<long> _genericCounter;
    private readonly Histogram<double> _genericHistogram;

    public ApplicationMetrics(IOptions<AwsConfig> awsConfig, IConfiguration configuration, IMeterFactory meterFactory)
    {
        var meterName = awsConfig.Value.Metrics.MeterName;
        var version = configuration.GetValue<string>("SERVICE_VERSION") ?? "1.0.0";
        
        _meter = meterFactory.Create(meterName, version);
        
        _requestCounter = _meter.CreateCounter<long>("requests_total", "request", "Total number of requests");
        _requestDuration = _meter.CreateHistogram<double>("request_duration_ms", "ms", "Request duration in milliseconds");
        _genericCounter = _meter.CreateCounter<long>("counter", "count", "Generic counter");
        _genericHistogram = _meter.CreateHistogram<double>("histogram", "value", "Generic histogram");
    }

    public void RecordRequest(string operation, string status)
    {
        _requestCounter.Add(1, 
            new KeyValuePair<string, object?>("operation", operation),
            new KeyValuePair<string, object?>("status", status));
    }

    public void RecordDuration(string operation, double durationMs)
    {
        _requestDuration.Record(durationMs,
            new KeyValuePair<string, object?>("operation", operation));
    }

    public void RecordCount(string metricName, long count = 1, params (string Key, string Value)[] tags)
    {
        var kvPairs = tags.Select(t => new KeyValuePair<string, object?>(t.Key, t.Value)).ToArray();
        var counter = _meter.CreateCounter<long>(metricName, "count", $"Counter for {metricName}");
        counter.Add(count, kvPairs);
    }

    public void RecordValue(string metricName, double value, params (string Key, string Value)[] tags)
    {
        var kvPairs = tags.Select(t => new KeyValuePair<string, object?>(t.Key, t.Value)).ToArray();
        var histogram = _meter.CreateHistogram<double>(metricName, "value", $"Histogram for {metricName}");
        histogram.Record(value, kvPairs);
    }

    public void Dispose()
    {
        _meter?.Dispose();
    }
}