using System.Diagnostics.Metrics;
using KeeperData.Infrastructure.Config;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KeeperData.Infrastructure.Metrics;

public class EmfMeterListener : IDisposable
{
    private readonly MeterListener _meterListener;
    private readonly IMetricsService _metricsService;
    private readonly ILogger<EmfMeterListener> _logger;
    private readonly string _meterName;

    public EmfMeterListener(
        MeterListener meterListener, 
        IMetricsService metricsService, 
        ILogger<EmfMeterListener> logger, 
        IOptions<AwsConfig> awsConfig)
    {
        _meterListener = meterListener;
        _metricsService = metricsService;
        _logger = logger;
        _meterName = awsConfig.Value.Metrics.MeterName;
        
        _meterListener.InstrumentPublished = OnInstrumentPublished;
        _meterListener.SetMeasurementEventCallback<double>(OnMeasurementRecorded);
        _meterListener.SetMeasurementEventCallback<int>(OnMeasurementRecorded);
        _meterListener.SetMeasurementEventCallback<long>(OnMeasurementRecorded);
        _meterListener.Start();
        
        _logger.LogDebug("EMF MeterListener started for meter: {MeterName}", _meterName);
    }

    private void OnInstrumentPublished(Instrument instrument, MeterListener listener)
    {
        if (instrument.Meter.Name == _meterName)
        {
            listener.EnableMeasurementEvents(instrument, null);
            _logger.LogDebug("Enabled measurement events for instrument: {InstrumentName}", instrument.Name);
        }
    }

    private void OnMeasurementRecorded<T>(Instrument instrument, T measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        where T : struct
    {
        try
        {
            var value = Convert.ToDouble(measurement);
            var dimensions = new Dictionary<string, string>();

            foreach (var tag in tags)
            {
                if (tag.Value?.ToString() is string stringValue)
                {
                    dimensions[tag.Key] = stringValue;
                }
            }

            var unit = DetermineUnit(instrument);
            _metricsService.PutMetric(instrument.Name, value, unit, dimensions);
            
            _logger.LogDebug("Recorded metric {InstrumentName}={Value} with {DimensionCount} dimensions", 
                instrument.Name, value, dimensions.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error recording measurement for instrument {InstrumentName}", instrument.Name);
        }
    }

    private static string DetermineUnit(Instrument instrument)
    {
        return instrument switch
        {
            _ when instrument.GetType().Name.Contains("Counter") => "Count",
            _ when instrument.Name.Contains("duration") || instrument.Name.Contains("time") => "Milliseconds",
            _ when instrument.Name.Contains("size") || instrument.Name.Contains("bytes") => "Bytes",
            _ => "Count"
        };
    }

    public void Dispose()
    {
        _meterListener?.Dispose();
        _logger.LogDebug("EMF MeterListener disposed");
    }
}