using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using KeeperData.Infrastructure.Config;
using Microsoft.Extensions.Options;

namespace KeeperData.Infrastructure.Telemetry;

[ExcludeFromCodeCoverage]
public class ApplicationMetrics : IApplicationMetrics
{
    private readonly Meter _meter;
    private readonly Counter<int> _requestCounter;
    private readonly Histogram<double> _durationHistogram;
    private readonly Counter<int> _generalCounter;
    private readonly Histogram<double> _generalHistogram;

    public ApplicationMetrics(IOptions<AwsConfig> awsConfig)
    {
        var meterName = awsConfig.Value.Metrics.MeterName;
        _meter = new Meter(meterName);

        // Create instruments for common metrics
        _requestCounter = _meter.CreateCounter<int>(
            name: "requests_total",
            unit: "ea",
            description: "Total number of requests by operation and status");

        _durationHistogram = _meter.CreateHistogram<double>(
            name: "duration_milliseconds",
            unit: "ms",
            description: "Duration of operations in milliseconds");

        _generalCounter = _meter.CreateCounter<int>(
            name: "count_total",
            unit: "ea",
            description: "General purpose counter with custom tags");

        _generalHistogram = _meter.CreateHistogram<double>(
            name: "value_measurement",
            unit: "value",
            description: "General purpose value measurement with custom tags");
    }

    public void RecordRequest(string operation, string status)
    {
        _requestCounter.Add(1,
            new KeyValuePair<string, object?>("operation", operation),
            new KeyValuePair<string, object?>("status", status));
    }

    public void RecordDuration(string operation, double durationMs)
    {
        _durationHistogram.Record(durationMs,
            new KeyValuePair<string, object?>("operation", operation));
    }

    public void RecordCount(string name, int value, params (string Key, string Value)[] tags)
    {
        var kvps = new List<KeyValuePair<string, object?>>
        {
            new("metric_name", name)
        };

        foreach (var (key, val) in tags)
        {
            kvps.Add(new KeyValuePair<string, object?>(key, val));
        }

        _generalCounter.Add(value, kvps.ToArray());
    }

    public void RecordValue(string name, double value, params (string Key, string Value)[] tags)
    {
        var kvps = new List<KeyValuePair<string, object?>>
        {
            new("metric_name", name)
        };

        foreach (var (key, val) in tags)
        {
            kvps.Add(new KeyValuePair<string, object?>(key, val));
        }

        _generalHistogram.Record(value, kvps.ToArray());
    }

    public void Dispose()
    {
        _meter?.Dispose();
    }
}