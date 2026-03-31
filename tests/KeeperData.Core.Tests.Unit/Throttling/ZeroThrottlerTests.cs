using FluentAssertions;
using KeeperData.Core.Throttling.Impl;

namespace KeeperData.Core.Tests.Unit.Throttling;

public class ZeroThrottlerTests
{
    private readonly ZeroThrottler _sut = new();

    [Fact]
    public async Task DelayAsync_ShouldReturnImmediately()
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();

        await _sut.DelayAsync(5000, CancellationToken.None);

        sw.Stop();
        sw.ElapsedMilliseconds.Should().BeLessThan(50);
    }

    [Fact]
    public void ActivePolicyName_ShouldBeZeroUnthrottled()
    {
        _sut.ActivePolicyName.Should().Be("Zero (Unthrottled)");
    }

    [Fact]
    public void ActivePolicySlug_ShouldBeZero()
    {
        _sut.ActivePolicySlug.Should().Be("zero");
    }

    [Fact]
    public void Settings_Ingestion_ShouldHaveZeroDelayAndMaxBatch()
    {
        var s = _sut.Settings.Ingestion;
        s.BatchSize.Should().Be(5000);
        s.BatchDelayMs.Should().Be(0);
        s.ProgressUpdateInterval.Should().Be(100);
        s.LogInterval.Should().Be(100);
    }

    [Fact]
    public void Settings_CleanseAnalysis_ShouldHaveZeroDelaysAndHighBatch()
    {
        var s = _sut.Settings.CleanseAnalysis;
        s.PumpBatchSize.Should().Be(2000);
        s.PumpDelayMs.Should().Be(0);
        s.RecordIssueDelayMs.Should().Be(0);
        s.ProgressUpdateInterval.Should().Be(50);
    }

    [Fact]
    public void Settings_CleanseExport_ShouldHaveZeroDelayAndMaxBatch()
    {
        var s = _sut.Settings.CleanseExport;
        s.StreamBatchSize.Should().Be(5000);
        s.ThrottlingDelayMs.Should().Be(0);
    }

    [Fact]
    public void Settings_IssueDeactivation_ShouldHaveZeroDelayAndMaxBatch()
    {
        var s = _sut.Settings.IssueDeactivation;
        s.BatchSize.Should().Be(5000);
        s.ThrottleDelayMs.Should().Be(0);
    }

    [Fact]
    public void Settings_IssueQuery_ShouldHaveMaxBatch()
    {
        _sut.Settings.IssueQuery.StreamBatchSize.Should().Be(5000);
    }

    [Fact]
    public void Settings_ShouldReturnSameInstance()
    {
        var first = _sut.Settings;
        var second = _sut.Settings;
        first.Should().BeSameAs(second);
    }
}
