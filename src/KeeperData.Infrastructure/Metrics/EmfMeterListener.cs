using System.Diagnostics.Metrics;
using Amazon.CloudWatch.EMF.Logger;
using Amazon.CloudWatch.EMF.Model;
using KeeperData.Infrastructure.Config;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Unit = Amazon.CloudWatch.EMF.Model.Unit;

namespace KeeperData.Infrastructure.Metrics;

public class EmfMeterListener : IDisposable
{
    private readonly MeterListener _meterListener;
    private readonly ILogger<EmfMeterListener> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly string _meterName;
    private readonly string _namespace;
    private readonly string _serviceName;

    public EmfMeterListener(
        MeterListener meterListener, 
        ILogger<EmfMeterListener> logger,
        ILoggerFactory loggerFactory,
        IOptions<AwsConfig> awsConfig)
    {
        _meterListener = meterListener;
        _logger = logger;
        _loggerFactory = loggerFactory;
        _meterName = awsConfig.Value.Metrics.MeterName;
        _namespace = awsConfig.Value.EMF.Namespace;
        _serviceName = awsConfig.Value.EMF.ServiceName;
        
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
            using var metricsLogger = new MetricsLogger(_loggerFactory);
            
            metricsLogger.SetNamespace(_namespace);
            var dimensionSet = new DimensionSet();
            dimensionSet.AddDimension("ServiceName", _serviceName);

            foreach (var tag in tags)
            {
                if (!string.IsNullOrWhiteSpace(tag.Value?.ToString()))
                {
                    dimensionSet.AddDimension(tag.Key, tag.Value!.ToString()!);
                }
            }

            metricsLogger.SetDimensions(dimensionSet);
            var name = instrument.Name;
            var value = Convert.ToDouble(measurement);
            var unit = DetermineUnit(instrument);

            metricsLogger.PutMetric(name, value, unit);
            metricsLogger.Flush();
            
            var tagCount = tags.Length;
            _logger.LogDebug("Recorded metric {InstrumentName}={Value} with {DimensionCount} dimensions", 
                instrument.Name, value, tagCount + 1); // +1 for ServiceName
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error recording measurement for instrument {InstrumentName}", instrument.Name);
        }
    }

    private static Unit DetermineUnit(Instrument instrument)
    {
        return instrument switch
        {
            _ when instrument.GetType().Name.Contains("Counter") => Unit.COUNT,
            _ when instrument.Name.Contains("duration") || instrument.Name.Contains("time") => Unit.MILLISECONDS,
            _ when instrument.Name.Contains("size") || instrument.Name.Contains("bytes") => Unit.BYTES,
            _ when instrument.Unit?.ToLowerInvariant() == "ms" => Unit.MILLISECONDS,
            _ when instrument.Unit?.ToLowerInvariant() == "s" => Unit.SECONDS,
            _ when !string.IsNullOrEmpty(instrument.Unit) && Enum.TryParse<Unit>(instrument.Unit, true, out var parsedUnit) => parsedUnit,
            _ => Unit.COUNT
        };
    }

    public void Dispose()
    {
        _meterListener?.Dispose();
        _logger.LogDebug("EMF MeterListener disposed");
    }
}