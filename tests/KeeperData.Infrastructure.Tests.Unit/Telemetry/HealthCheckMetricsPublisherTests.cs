using System.Diagnostics.Metrics;
using FluentAssertions;
using KeeperData.Core.Telemetry;
using KeeperData.Infrastructure.Telemetry;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Telemetry;

public class HealthCheckMetricsPublisherTests : IDisposable
{
    private readonly TestMeterFactoryForPublisher _meterFactory;
    private readonly MeterListener _meterListener;
    private readonly List<(string InstrumentName, object Value)> _measurements;
    private readonly HealthCheckMetrics _healthMetrics;
    private readonly Mock<IApplicationMetrics> _applicationMetricsMock;
    private readonly Mock<ILogger<HealthCheckMetricsPublisher>> _loggerMock;
    private readonly HealthCheckMetricsPublisher _sut;

    public HealthCheckMetricsPublisherTests()
    {
        _meterFactory = new TestMeterFactoryForPublisher();
        _measurements = [];
        _meterListener = new MeterListener();

        _meterListener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "KeeperData")
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };

        _meterListener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
        {
            _measurements.Add((instrument.Name, measurement));
        });

        _meterListener.SetMeasurementEventCallback<double>((instrument, measurement, tags, state) =>
        {
            _measurements.Add((instrument.Name, measurement));
        });

        _meterListener.SetMeasurementEventCallback<int>((instrument, measurement, tags, state) =>
        {
            _measurements.Add((instrument.Name, measurement));
        });

        _meterListener.Start();

        _healthMetrics = new HealthCheckMetrics(_meterFactory);
        _applicationMetricsMock = new Mock<IApplicationMetrics>();
        _loggerMock = new Mock<ILogger<HealthCheckMetricsPublisher>>();

        _sut = new HealthCheckMetricsPublisher(
            _healthMetrics,
            _applicationMetricsMock.Object,
            _loggerMock.Object);
    }

    public void Dispose()
    {
        _meterListener.Dispose();
        _meterFactory.Dispose();
    }

    [Fact]
    public async Task PublishAsync_WithSingleHealthyEntry_RecordsMetrics()
    {
        // Arrange
        var report = CreateHealthReport(
            HealthStatus.Healthy,
            ("mongodb", HealthStatus.Healthy, TimeSpan.FromMilliseconds(50)));

        // Act
        await _sut.PublishAsync(report, CancellationToken.None);

        // Assert - verify health check metrics were recorded
        _measurements.Should().Contain(m => m.InstrumentName == "keeperdata.health.checks.total");
        _measurements.Should().Contain(m => m.InstrumentName == "keeperdata.health.checks.duration");
    }

    [Fact]
    public async Task PublishAsync_WithMultipleEntries_RecordsMetricsForEach()
    {
        // Arrange
        var report = CreateHealthReport(
            HealthStatus.Healthy,
            ("mongodb", HealthStatus.Healthy, TimeSpan.FromMilliseconds(50)),
            ("s3", HealthStatus.Healthy, TimeSpan.FromMilliseconds(100)),
            ("sns", HealthStatus.Degraded, TimeSpan.FromMilliseconds(200)));

        // Act
        await _sut.PublishAsync(report, CancellationToken.None);

        // Assert - 3 entries + 1 overall = 4 total measurements for each metric type
        var totalMeasurements = _measurements.Where(m => m.InstrumentName == "keeperdata.health.checks.total");
        totalMeasurements.Should().HaveCount(4);
    }

    [Fact]
    public async Task PublishAsync_RecordsApplicationMetricsForEachEntry()
    {
        // Arrange
        var report = CreateHealthReport(
            HealthStatus.Healthy,
            ("mongodb", HealthStatus.Healthy, TimeSpan.FromMilliseconds(50)));

        // Act
        await _sut.PublishAsync(report, CancellationToken.None);

        // Assert
        _applicationMetricsMock.Verify(m => m.RecordRequest(MetricNames.HealthCheck, "healthy"), Times.AtLeastOnce);
        _applicationMetricsMock.Verify(m => m.RecordDuration("health_check", 50), Times.Once);
    }

    [Fact]
    public async Task PublishAsync_RecordsOverallHealthCheckCount()
    {
        // Arrange
        var report = CreateHealthReport(
            HealthStatus.Healthy,
            ("mongodb", HealthStatus.Healthy, TimeSpan.FromMilliseconds(50)),
            ("s3", HealthStatus.Healthy, TimeSpan.FromMilliseconds(100)));

        // Act
        await _sut.PublishAsync(report, CancellationToken.None);

        // Assert
        _applicationMetricsMock.Verify(m => m.RecordCount(
            MetricNames.HealthCheck, 
            2, 
            It.IsAny<(string, string)>()), Times.Once);
    }

    [Fact]
    public async Task PublishAsync_WhenRecordDurationThrows_DoesNotThrow()
    {
        // Arrange - only RecordDuration throws, RecordRequest works (so error recording succeeds)
        _applicationMetricsMock.Setup(m => m.RecordDuration(It.IsAny<string>(), It.IsAny<double>()))
            .Throws(new InvalidOperationException("Duration recording error"));

        var report = CreateHealthReport(
            HealthStatus.Healthy,
            ("mongodb", HealthStatus.Healthy, TimeSpan.FromMilliseconds(50)));

        // Act
        var act = () => _sut.PublishAsync(report, CancellationToken.None);

        // Assert - should not throw because exception is caught and error metric is recorded
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task PublishAsync_WhenExceptionOccurs_RecordsErrorMetric()
    {
        // Arrange
        _applicationMetricsMock.Setup(m => m.RecordDuration(It.IsAny<string>(), It.IsAny<double>()))
            .Throws(new InvalidOperationException("Metrics error"));

        var report = CreateHealthReport(
            HealthStatus.Healthy,
            ("mongodb", HealthStatus.Healthy, TimeSpan.FromMilliseconds(50)));

        // Act
        await _sut.PublishAsync(report, CancellationToken.None);

        // Assert
        _applicationMetricsMock.Verify(m => m.RecordRequest(MetricNames.HealthCheck, "error"), Times.Once);
    }

    [Fact]
    public async Task PublishAsync_WithUnhealthyOverallStatus_RecordsCorrectStatus()
    {
        // Arrange
        var report = CreateHealthReport(
            HealthStatus.Unhealthy,
            ("mongodb", HealthStatus.Unhealthy, TimeSpan.FromMilliseconds(50)));

        // Act
        await _sut.PublishAsync(report, CancellationToken.None);

        // Assert - verify failure metric was recorded
        _measurements.Should().Contain(m => m.InstrumentName == "keeperdata.health.checks.failures");
        _applicationMetricsMock.Verify(m => m.RecordRequest(MetricNames.HealthCheck, "unhealthy"), Times.AtLeastOnce);
    }

    [Fact]
    public async Task PublishAsync_RecordsOverallDuration()
    {
        // Arrange
        var report = CreateHealthReport(
            HealthStatus.Healthy,
            ("mongodb", HealthStatus.Healthy, TimeSpan.FromMilliseconds(50)));

        // Act
        await _sut.PublishAsync(report, CancellationToken.None);

        // Assert
        _applicationMetricsMock.Verify(m => m.RecordDuration("health_check_overall", It.IsAny<double>()), Times.Once);
    }

    [Fact]
    public async Task PublishAsync_WithEmptyReport_RecordsOnlyOverallMetrics()
    {
        // Arrange
        var report = new HealthReport(
            new Dictionary<string, HealthReportEntry>(),
            TimeSpan.FromMilliseconds(10));

        // Act
        await _sut.PublishAsync(report, CancellationToken.None);

        // Assert - overall status metric should be recorded
        var totalMeasurements = _measurements.Where(m => m.InstrumentName == "keeperdata.health.checks.total");
        totalMeasurements.Should().HaveCount(1); // Just the "overall" measurement
        _applicationMetricsMock.Verify(m => m.RecordCount(MetricNames.HealthCheck, 0, It.IsAny<(string, string)>()), Times.Once);
    }

    private static HealthReport CreateHealthReport(
        HealthStatus overallStatus,
        params (string Name, HealthStatus Status, TimeSpan Duration)[] entries)
    {
        var healthEntries = new Dictionary<string, HealthReportEntry>();
        var totalDuration = TimeSpan.Zero;

        foreach (var (name, status, duration) in entries)
        {
            healthEntries[name] = new HealthReportEntry(
                status,
                description: null,
                duration,
                exception: null,
                data: null);
            totalDuration += duration;
        }

        return new HealthReport(healthEntries, totalDuration);
    }
}

/// <summary>
/// Simple test meter factory for unit testing metrics.
/// </summary>
internal sealed class TestMeterFactoryForPublisher : IMeterFactory
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
