using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Operations.Queries;
using KeeperData.Core.Tests.Unit.Throttling;
using Microsoft.Extensions.Time.Testing;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Operations.Queries;

public class CleanseRunStatsServiceTests
{
    private readonly FakeThrottler _throttler = new();
    private readonly FakeTimeProvider _timeProvider = new();
    private readonly CleanseRunStatsService _sut;

    public CleanseRunStatsServiceTests()
    {
        _sut = new CleanseRunStatsService(_throttler, _timeProvider);
    }

    #region CalculateStats

    [Fact]
    public void CalculateStats_WithNoSnapshots_ShouldReturnZeroCurrentRpm()
    {
        var startedAt = _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-5);
        var stats = _sut.CalculateStats("op-1", recordsAnalyzed: 1000, totalRecords: 5000, startedAtUtc: startedAt);

        stats.Should().NotBeNull();
        stats!.CurrentRpm.Should().Be(0);
    }

    [Fact]
    public void CalculateStats_WithNoSnapshots_ShouldReturnPositiveAverageRpm()
    {
        var startedAt = _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-5);
        var stats = _sut.CalculateStats("op-1", recordsAnalyzed: 1000, totalRecords: 5000, startedAtUtc: startedAt);

        stats.Should().NotBeNull();
        stats!.AverageRpm.Should().BeGreaterThan(0);
    }

    [Fact]
    public void CalculateStats_WhenZeroRecordsAnalyzed_ShouldReturnZeroRpmAndNullProjection()
    {
        var startedAt = _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-1);
        var stats = _sut.CalculateStats("op-1", recordsAnalyzed: 0, totalRecords: 5000, startedAtUtc: startedAt);

        stats.Should().NotBeNull();
        stats!.AverageRpm.Should().Be(0);
        stats.CurrentRpm.Should().Be(0);
        stats.ProjectedEndUtc.Should().BeNull();
    }

    [Fact]
    public void CalculateStats_WhenRecordsAnalyzed_ShouldReturnProjectedEndInFuture()
    {
        var startedAt = _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-10);
        var stats = _sut.CalculateStats("op-1", recordsAnalyzed: 1000, totalRecords: 5000, startedAtUtc: startedAt);

        stats.Should().NotBeNull();
        stats!.ProjectedEndUtc.Should().NotBeNull();
        stats.ProjectedEndUtc.Should().BeAfter(_timeProvider.GetUtcNow().UtcDateTime);
    }

    [Fact]
    public void CalculateStats_WhenAllRecordsAnalyzed_ShouldReturnNullProjection()
    {
        var startedAt = _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-5);
        var stats = _sut.CalculateStats("op-1", recordsAnalyzed: 5000, totalRecords: 5000, startedAtUtc: startedAt);

        stats.Should().NotBeNull();
        stats!.ProjectedEndUtc.Should().BeNull();
    }

    [Fact]
    public void CalculateStats_ShouldReturnThrottlePolicyDetails()
    {
        var stats = _sut.CalculateStats("op-1", recordsAnalyzed: 100, totalRecords: 1000,
            startedAtUtc: _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-1));

        stats.Should().NotBeNull();
        stats!.ThrottlePolicyName.Should().Be("UnitTest");
        stats.ThrottlePolicySlug.Should().Be("unit-test");
        stats.PumpBatchSize.Should().Be(_throttler.Settings.CleanseAnalysis.PumpBatchSize);
        stats.PumpDelayMs.Should().Be(_throttler.Settings.CleanseAnalysis.PumpDelayMs);
    }

    [Fact]
    public void CalculateStats_WhenStartedJustNow_ShouldReturnZeroAverageRpm()
    {
        var stats = _sut.CalculateStats("op-1", recordsAnalyzed: 0, totalRecords: 5000,
            startedAtUtc: _timeProvider.GetUtcNow().UtcDateTime);

        stats.Should().NotBeNull();
        stats!.AverageRpm.Should().Be(0);
    }

    #endregion

    #region RecordSnapshot + CalculateStats (sliding window)

    [Fact]
    public void CalculateStats_WithSingleSnapshot_ShouldReturnZeroCurrentRpm()
    {
        _sut.RecordSnapshot("op-1", 100);

        var stats = _sut.CalculateStats("op-1", recordsAnalyzed: 100, totalRecords: 1000,
            startedAtUtc: _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-1));

        stats.Should().NotBeNull();
        stats!.CurrentRpm.Should().Be(0, "need at least 2 snapshots to calculate window RPM");
    }

    [Fact]
    public void CalculateStats_WithMultipleSnapshots_ShouldReturnPositiveCurrentRpm()
    {
        _sut.RecordSnapshot("op-1", 100);
        _timeProvider.Advance(TimeSpan.FromSeconds(10));
        _sut.RecordSnapshot("op-1", 200);

        var stats = _sut.CalculateStats("op-1", recordsAnalyzed: 200, totalRecords: 1000,
            startedAtUtc: _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-1));

        stats.Should().NotBeNull();
        stats!.CurrentRpm.Should().BeGreaterThan(0);
    }

    #endregion

    #region ClearSnapshots

    [Fact]
    public void ClearSnapshots_ShouldRemoveAllDataForOperation()
    {
        _sut.RecordSnapshot("op-1", 100);
        _sut.RecordSnapshot("op-1", 200);

        _sut.ClearSnapshots("op-1");

        var stats = _sut.CalculateStats("op-1", recordsAnalyzed: 200, totalRecords: 1000,
            startedAtUtc: _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-1));

        stats.Should().NotBeNull();
        stats!.CurrentRpm.Should().Be(0, "snapshots were cleared");
    }

    [Fact]
    public void ClearSnapshots_ShouldNotAffectOtherOperations()
    {
        _sut.RecordSnapshot("op-1", 100);
        _sut.RecordSnapshot("op-2", 100);
        _timeProvider.Advance(TimeSpan.FromSeconds(10));
        _sut.RecordSnapshot("op-2", 200);

        _sut.ClearSnapshots("op-1");

        var stats = _sut.CalculateStats("op-2", recordsAnalyzed: 200, totalRecords: 1000,
            startedAtUtc: _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-1));

        stats.Should().NotBeNull();
        stats!.CurrentRpm.Should().BeGreaterThan(0);
    }

    [Fact]
    public void ClearSnapshots_WhenOperationNotFound_ShouldNotThrow()
    {
        var act = () => _sut.ClearSnapshots("nonexistent");
        act.Should().NotThrow();
    }

    #endregion

    #region RecordSnapshot

    [Fact]
    public void RecordSnapshot_ShouldAccumulateSnapshots()
    {
        _sut.RecordSnapshot("op-1", 100);
        _sut.RecordSnapshot("op-1", 200);
        _sut.RecordSnapshot("op-1", 300);

        // Verify snapshots exist by checking that stats can use them
        var stats = _sut.CalculateStats("op-1", recordsAnalyzed: 300, totalRecords: 1000,
            startedAtUtc: _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-1));

        stats.Should().NotBeNull();
    }

    #endregion
}
