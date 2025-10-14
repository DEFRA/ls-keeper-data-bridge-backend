using Amazon.CloudWatch;
using Amazon.CloudWatch.Model;
using KeeperData.Infrastructure.Config;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;

namespace KeeperData.Infrastructure.Metrics;

[ExcludeFromCodeCoverage]
public class CloudWatchMeterListener : BackgroundService
{
    private readonly MeterListener _meterListener;
    private readonly IAmazonCloudWatch _cloudWatch;
    private readonly ILogger<CloudWatchMeterListener> _logger;
    private readonly AwsConfig _config;
    private readonly ConcurrentQueue<MetricDatum> _metricQueue = new();
    private readonly Timer _publishTimer;

    public CloudWatchMeterListener(
        IAmazonCloudWatch cloudWatch,
        ILogger<CloudWatchMeterListener> logger,
        IOptions<AwsConfig> config)
    {
        _cloudWatch = cloudWatch;
        _logger = logger;
        _config = config.Value;
        _meterListener = new MeterListener();
        
        // Publish metrics every 30 seconds
        _publishTimer = new Timer(PublishQueuedMetrics, null, TimeSpan.Zero, TimeSpan.FromSeconds(30));
        
        SetupMeterListener();
    }

    private void SetupMeterListener()
    {
        _meterListener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "KeeperData.Application")
            {
                listener.EnableMeasurementEvents(instrument, null);
            }
        };

        _meterListener.SetMeasurementEventCallback<long>(OnMeasurementRecorded);
        _meterListener.SetMeasurementEventCallback<double>(OnMeasurementRecorded);
        _meterListener.SetMeasurementEventCallback<int>(OnMeasurementRecorded);
    }

    private void OnMeasurementRecorded<T>(
        Instrument instrument,
        T measurement,
        ReadOnlySpan<KeyValuePair<string, object?>> tags,
        object? state) where T : struct
    {
        try
        {
            var value = Convert.ToDouble(measurement);
            var unit = GetStandardUnit(instrument);
            
            var dimensions = new List<Dimension>();
            foreach (var tag in tags)
            {
                if (tag.Value != null)
                {
                    dimensions.Add(new Dimension 
                    { 
                        Name = tag.Key, 
                        Value = tag.Value.ToString() ?? string.Empty 
                    });
                }
            }

            var metricDatum = new MetricDatum
            {
                MetricName = instrument.Name,
                Value = value,
                Unit = unit,
                Timestamp = DateTime.UtcNow,
                Dimensions = dimensions
            };

            _metricQueue.Enqueue(metricDatum);

            _logger.LogTrace("Queued metric {MetricName} with value {Value} for publishing", 
                instrument.Name, value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing measurement for instrument {InstrumentName}", 
                instrument.Name);
        }
    }

    private StandardUnit GetStandardUnit(Instrument instrument)
    {
        // Map instrument units to CloudWatch standard units
        if (instrument.Unit?.Contains("millisecond") == true || instrument.Unit?.Contains("ms") == true)
            return StandardUnit.Milliseconds;
        if (instrument.Unit?.Contains("second") == true)
            return StandardUnit.Seconds;
        if (instrument.Unit?.Contains("count") == true || instrument.Name.EndsWith("_total"))
            return StandardUnit.Count;
        
        return StandardUnit.None;
    }

    private async void PublishQueuedMetrics(object? state)
    {
        if (_metricQueue.IsEmpty) return;

        try
        {
            var metricsToPublish = new List<MetricDatum>();
            
            // Dequeue up to 20 metrics (CloudWatch API limit)
            while (metricsToPublish.Count < 20 && _metricQueue.TryDequeue(out var metric))
            {
                metricsToPublish.Add(metric);
            }

            if (metricsToPublish.Count == 0) return;

            var request = new PutMetricDataRequest
            {
                Namespace = GetNamespace(),
                MetricData = metricsToPublish
            };

            await _cloudWatch.PutMetricDataAsync(request);

            _logger.LogDebug("Successfully published {MetricCount} metrics to CloudWatch namespace {Namespace}",
                metricsToPublish.Count, GetNamespace());
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish queued metrics to CloudWatch");
        }
    }

    private string GetNamespace()
    {
        return _config.EMF.Namespace;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _meterListener.Start();
        _logger.LogInformation("CloudWatch MeterListener started");

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("CloudWatch MeterListener stopping");
        }
    }

    public override void Dispose()
    {
        _publishTimer?.Dispose();
        _meterListener?.Dispose();
        base.Dispose();
    }
}