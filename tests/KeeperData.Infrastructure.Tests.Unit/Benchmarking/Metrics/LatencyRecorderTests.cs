using FluentAssertions;
using KeeperData.Infrastructure.Benchmarking.Metrics;

namespace KeeperData.Infrastructure.Tests.Unit.Benchmarking.Metrics;

public class LatencyRecorderTests
{
    [Fact]
    public void Compute_EmptyRecorder_ReturnsDefaultStats()
    {
        var recorder = new LatencyRecorder();

        var stats = recorder.Compute();

        stats.AvgMs.Should().Be(0);
        stats.P50Ms.Should().Be(0);
        stats.P95Ms.Should().Be(0);
        stats.P99Ms.Should().Be(0);
        stats.MinMs.Should().Be(0);
        stats.MaxMs.Should().Be(0);
    }

    [Fact]
    public void Count_EmptyRecorder_ReturnsZero()
    {
        var recorder = new LatencyRecorder();

        recorder.Count.Should().Be(0);
    }

    [Fact]
    public void Compute_SingleSample_AllPercentilesEqualSample()
    {
        var recorder = new LatencyRecorder();

        recorder.Record(TimeSpan.FromMilliseconds(42));

        var stats = recorder.Compute();
        stats.AvgMs.Should().Be(42);
        stats.P50Ms.Should().Be(42);
        stats.P95Ms.Should().Be(42);
        stats.P99Ms.Should().Be(42);
        stats.MinMs.Should().Be(42);
        stats.MaxMs.Should().Be(42);
    }

    [Fact]
    public void Count_AfterRecording_ReflectsSampleCount()
    {
        var recorder = new LatencyRecorder();

        recorder.Record(TimeSpan.FromMilliseconds(1));
        recorder.Record(TimeSpan.FromMilliseconds(2));
        recorder.Record(TimeSpan.FromMilliseconds(3));

        recorder.Count.Should().Be(3);
    }

    [Fact]
    public void Compute_MultipleSamples_CalculatesCorrectStats()
    {
        var recorder = new LatencyRecorder();

        // Record 100 samples: 1ms, 2ms, ..., 100ms
        for (var i = 1; i <= 100; i++)
            recorder.Record(TimeSpan.FromMilliseconds(i));

        var stats = recorder.Compute();

        stats.MinMs.Should().Be(1);
        stats.MaxMs.Should().Be(100);
        stats.AvgMs.Should().Be(50.5);
        stats.P50Ms.Should().BeApproximately(50.5, 0.5);
        stats.P95Ms.Should().BeApproximately(95.05, 0.5);
        stats.P99Ms.Should().BeApproximately(99.01, 0.5);
    }

    [Fact]
    public void Compute_TwoSamples_InterpolatesPercentiles()
    {
        var recorder = new LatencyRecorder();

        recorder.Record(TimeSpan.FromMilliseconds(10));
        recorder.Record(TimeSpan.FromMilliseconds(20));

        var stats = recorder.Compute();

        stats.MinMs.Should().Be(10);
        stats.MaxMs.Should().Be(20);
        stats.AvgMs.Should().Be(15);
        stats.P50Ms.Should().Be(15); // interpolated midpoint
    }

    [Fact]
    public void Compute_RoundsToTwoDecimalPlaces()
    {
        var recorder = new LatencyRecorder();

        recorder.Record(TimeSpan.FromTicks(33333)); // ~3.3333ms

        var stats = recorder.Compute();

        // All values should be rounded to 2 decimal places
        stats.AvgMs.Should().Be(Math.Round(stats.AvgMs, 2));
        stats.P50Ms.Should().Be(Math.Round(stats.P50Ms, 2));
        stats.MinMs.Should().Be(Math.Round(stats.MinMs, 2));
        stats.MaxMs.Should().Be(Math.Round(stats.MaxMs, 2));
    }

    [Fact]
    public void Compute_IdenticalSamples_AllStatsEqual()
    {
        var recorder = new LatencyRecorder();

        for (var i = 0; i < 50; i++)
            recorder.Record(TimeSpan.FromMilliseconds(7));

        var stats = recorder.Compute();

        stats.AvgMs.Should().Be(7);
        stats.P50Ms.Should().Be(7);
        stats.P95Ms.Should().Be(7);
        stats.P99Ms.Should().Be(7);
        stats.MinMs.Should().Be(7);
        stats.MaxMs.Should().Be(7);
    }
}
