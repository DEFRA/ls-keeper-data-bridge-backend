using System.Diagnostics.Metrics;
using FluentAssertions;
using KeeperData.Core.Telemetry;
using KeeperData.Infrastructure.Telemetry;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace KeeperData.Infrastructure.Tests.Unit.Telemetry;

public class HealthCheckMetricsTests : IDisposable
{
    private readonly TestMeterFactory _meterFactory;
    private readonly MeterListener _meterListener;
    private readonly List<MeasurementRecord> _measurements;
    private readonly HealthCheckMetrics _sut;

    // Use the actual tag names from MetricNames.CommonTags
    private const string ServiceTagName = MetricNames.CommonTags.Service;        // "keeperdata.service"
    private const string HealthCheckTagName = MetricNames.CommonTags.HealthCheck; // "keeperdata.healthcheck"
    private const string StatusTagName = MetricNames.CommonTags.Status;           // "keeperdata.status"

    public HealthCheckMetricsTests()
    {
        _meterFactory = new TestMeterFactory();
        _measurements = [];
        _meterListener = new MeterListener();

        // Configure listener to capture all measurements from our meter
        _meterListener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "KeeperData")
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };

        _meterListener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
        {
            _measurements.Add(new MeasurementRecord(instrument.Name, measurement, tags.ToArray()));
        });

        _meterListener.SetMeasurementEventCallback<double>((instrument, measurement, tags, state) =>
        {
            _measurements.Add(new MeasurementRecord(instrument.Name, measurement, tags.ToArray()));
        });

        _meterListener.SetMeasurementEventCallback<int>((instrument, measurement, tags, state) =>
        {
            _measurements.Add(new MeasurementRecord(instrument.Name, measurement, tags.ToArray()));
        });

        _meterListener.Start();

        _sut = new HealthCheckMetrics(_meterFactory);
    }

    public void Dispose()
    {
        _meterListener.Dispose();
        _meterFactory.Dispose();
    }

    [Fact]
    public void RecordHealthCheck_WithHealthyStatus_RecordsTotalCounter()
    {
        // Act
        _sut.RecordHealthCheck("mongodb", HealthStatus.Healthy, 50.0);

        // Assert
        var measurement = _measurements.FirstOrDefault(m => m.InstrumentName == "keeperdata.health.checks.total");
        measurement.Should().NotBeNull();
        measurement!.Value.Should().Be(1L);
        measurement.GetTagValue(HealthCheckTagName).Should().Be("mongodb");
        measurement.GetTagValue(StatusTagName).Should().Be("Healthy");
    }

    [Fact]
    public void RecordHealthCheck_WithHealthyStatus_RecordsDuration()
    {
        // Act
        _sut.RecordHealthCheck("mongodb", HealthStatus.Healthy, 123.45);

        // Assert
        var measurement = _measurements.FirstOrDefault(m => m.InstrumentName == "keeperdata.health.checks.duration");
        measurement.Should().NotBeNull();
        measurement!.Value.Should().Be(123.45);
    }

    [Fact]
    public void RecordHealthCheck_WithHealthyStatus_DoesNotRecordFailure()
    {
        // Act
        _sut.RecordHealthCheck("mongodb", HealthStatus.Healthy, 50.0);

        // Assert
        var failureMeasurements = _measurements.Where(m => m.InstrumentName == "keeperdata.health.checks.failures");
        failureMeasurements.Should().BeEmpty();
    }

    [Fact]
    public void RecordHealthCheck_WithUnhealthyStatus_RecordsFailureCounter()
    {
        // Act
        _sut.RecordHealthCheck("mongodb", HealthStatus.Unhealthy, 50.0);

        // Assert
        var measurement = _measurements.FirstOrDefault(m => m.InstrumentName == "keeperdata.health.checks.failures");
        measurement.Should().NotBeNull();
        measurement!.Value.Should().Be(1L);
        measurement.GetTagValue(HealthCheckTagName).Should().Be("mongodb");
        measurement.GetTagValue(StatusTagName).Should().Be("Unhealthy");
    }

    [Fact]
    public void RecordHealthCheck_WithDegradedStatus_RecordsFailureCounter()
    {
        // Act
        _sut.RecordHealthCheck("s3", HealthStatus.Degraded, 500.0);

        // Assert
        var measurement = _measurements.FirstOrDefault(m => m.InstrumentName == "keeperdata.health.checks.failures");
        measurement.Should().NotBeNull();
        measurement!.Value.Should().Be(1L);
        measurement.GetTagValue(StatusTagName).Should().Be("Degraded");
    }

    [Theory]
    [InlineData(HealthStatus.Healthy, 2)]
    [InlineData(HealthStatus.Degraded, 1)]
    [InlineData(HealthStatus.Unhealthy, 0)]
    public void RecordHealthCheck_MapsStatusToCorrectNumericValue(HealthStatus status, int expectedValue)
    {
        // Act
        _sut.RecordHealthCheck("test-check", status, 100.0);

        // Assert
        var measurement = _measurements.FirstOrDefault(m => m.InstrumentName == "keeperdata.health.status");
        measurement.Should().NotBeNull();
        measurement!.Value.Should().Be(expectedValue);
    }

    [Fact]
    public void RecordHealthCheck_IncludesServiceTag()
    {
        // Act
        _sut.RecordHealthCheck("mongodb", HealthStatus.Healthy, 50.0);

        // Assert
        var measurement = _measurements.FirstOrDefault(m => m.InstrumentName == "keeperdata.health.checks.total");
        measurement.Should().NotBeNull();
        measurement!.Tags.Should().Contain(t => t.Key == ServiceTagName);
    }

    [Fact]
    public void RecordHealthCheck_MultipleCallsRecordMultipleMeasurements()
    {
        // Act
        _sut.RecordHealthCheck("mongodb", HealthStatus.Healthy, 50.0);
        _sut.RecordHealthCheck("mongodb", HealthStatus.Healthy, 60.0);
        _sut.RecordHealthCheck("s3", HealthStatus.Unhealthy, 100.0);

        // Assert
        var totalMeasurements = _measurements.Where(m => m.InstrumentName == "keeperdata.health.checks.total");
        totalMeasurements.Should().HaveCount(3);

        var failureMeasurements = _measurements.Where(m => m.InstrumentName == "keeperdata.health.checks.failures");
        failureMeasurements.Should().HaveCount(1);

        var durationMeasurements = _measurements.Where(m => m.InstrumentName == "keeperdata.health.checks.duration");
        durationMeasurements.Should().HaveCount(3);
    }

    [Fact]
    public void RecordHealthCheck_WithDifferentHealthCheckNames_TagsCorrectly()
    {
        // Act
        _sut.RecordHealthCheck("mongodb", HealthStatus.Healthy, 50.0);
        _sut.RecordHealthCheck("s3", HealthStatus.Healthy, 75.0);
        _sut.RecordHealthCheck("sns", HealthStatus.Degraded, 200.0);

        // Assert
        var measurements = _measurements.Where(m => m.InstrumentName == "keeperdata.health.checks.total").ToList();
        measurements.Should().HaveCount(3);
        measurements.Select(m => m.GetTagValue(HealthCheckTagName)).Should().BeEquivalentTo(["mongodb", "s3", "sns"]);
    }

    [Fact]
    public void Constructor_CreatesAllRequiredMetrics()
    {
        // Act
        _sut.RecordHealthCheck("test", HealthStatus.Unhealthy, 100.0);

        // Assert - verify all four metric types are recorded
        _measurements.Should().Contain(m => m.InstrumentName == "keeperdata.health.checks.total");
        _measurements.Should().Contain(m => m.InstrumentName == "keeperdata.health.checks.failures");
        _measurements.Should().Contain(m => m.InstrumentName == "keeperdata.health.checks.duration");
        _measurements.Should().Contain(m => m.InstrumentName == "keeperdata.health.status");
    }

    private record MeasurementRecord(string InstrumentName, object Value, KeyValuePair<string, object?>[] Tags)
    {
        public object? GetTagValue(string key) => Tags.FirstOrDefault(t => t.Key == key).Value;
    }
}

/// <summary>
/// Simple test meter factory for unit testing metrics.
/// </summary>
internal sealed class TestMeterFactory : IMeterFactory
{
    private readonly List<Meter> _meters = [];

    public Meter Create(MeterOptions options)
    {
        var meter = new Meter(options.Name, options.Version);
        _meters.Add(meter);
        return meter;
    }

    public void Dispose()
    {
        foreach (var meter in _meters)
        {
            meter.Dispose();
        }
        _meters.Clear();
    }
}
