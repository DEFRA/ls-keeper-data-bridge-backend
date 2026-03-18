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
        stats.EstimatedDurationRemainingSeconds.Should().BeNull();
    }

    [Fact]
    public void CalculateStats_WithWindowData_ShouldUseWindowRpmForProjection()
    {
        _sut.RecordSnapshot("op-1", 100);
        _timeProvider.Advance(TimeSpan.FromSeconds(10));
        _sut.RecordSnapshot("op-1", 200);

        var stats = _sut.CalculateStats("op-1", recordsAnalyzed: 200, totalRecords: 1000,
            startedAtUtc: _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-1));

        stats.Should().NotBeNull();
        stats!.ProjectedEndUtc.Should().NotBeNull();
        stats.EstimatedDurationRemainingSeconds.Should().NotBeNull();
        stats.EstimatedDurationRemainingSeconds.Should().BeGreaterThan(0);
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

    #region Phase-keyed snapshots and stats

    [Fact]
    public void RecordSnapshot_WithPhaseName_ShouldAccumulatePerPhase()
    {
        _sut.RecordSnapshot("op-1", "Analysis", 100);
        _timeProvider.Advance(TimeSpan.FromSeconds(10));
        _sut.RecordSnapshot("op-1", "Analysis", 200);

        var stats = _sut.CalculatePhaseStats("op-1", "Analysis",
            recordsProcessed: 200, totalRecords: 1000,
            phaseStartedAtUtc: _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-1));

        stats.Should().NotBeNull();
        stats!.CurrentRpm.Should().BeGreaterThan(0);
    }

    [Fact]
    public void CalculatePhaseStats_WithNoSnapshots_ShouldReturnZeroCurrentRpm()
    {
        var stats = _sut.CalculatePhaseStats("op-1", "Deactivation",
            recordsProcessed: 50, totalRecords: 100,
            phaseStartedAtUtc: _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-1));

        stats.Should().NotBeNull();
        stats!.CurrentRpm.Should().Be(0);
        stats.AverageRpm.Should().BeGreaterThan(0);
    }

    [Fact]
    public void CalculatePhaseStats_ShouldReturnThrottleSettingsForPhase()
    {
        var deactivationStats = _sut.CalculatePhaseStats("op-1", "Deactivation",
            recordsProcessed: 50, totalRecords: 100,
            phaseStartedAtUtc: _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-1));

        deactivationStats.Should().NotBeNull();
        deactivationStats!.BatchSize.Should().Be(_throttler.Settings.IssueDeactivation.BatchSize);
        deactivationStats.BatchDelayMs.Should().Be(_throttler.Settings.IssueDeactivation.ThrottleDelayMs);
    }

    [Fact]
    public void CalculatePhaseStats_ForExport_ShouldReturnExportThrottleSettings()
    {
        var exportStats = _sut.CalculatePhaseStats("op-1", "Export",
            recordsProcessed: 100, totalRecords: 500,
            phaseStartedAtUtc: _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-1));

        exportStats.Should().NotBeNull();
        exportStats!.BatchSize.Should().Be(_throttler.Settings.CleanseExport.StreamBatchSize);
        exportStats.BatchDelayMs.Should().Be(_throttler.Settings.CleanseExport.ThrottlingDelayMs);
    }

    [Fact]
    public void CalculatePhaseStats_WhenAllProcessed_ShouldReturnNullProjection()
    {
        var stats = _sut.CalculatePhaseStats("op-1", "Analysis",
            recordsProcessed: 1000, totalRecords: 1000,
            phaseStartedAtUtc: _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-5));

        stats.Should().NotBeNull();
        stats!.ProjectedEndUtc.Should().BeNull();
        stats.EstimatedRemainingSeconds.Should().BeNull();
    }

    [Fact]
    public void ClearSnapshots_ShouldRemoveAllPhaseKeysForOperation()
    {
        _sut.RecordSnapshot("op-1", "Analysis", 100);
        _sut.RecordSnapshot("op-1", "Deactivation", 50);
        _timeProvider.Advance(TimeSpan.FromSeconds(10));
        _sut.RecordSnapshot("op-1", "Analysis", 200);
        _sut.RecordSnapshot("op-1", "Deactivation", 100);

        _sut.ClearSnapshots("op-1");

        var analysisStats = _sut.CalculatePhaseStats("op-1", "Analysis",
            recordsProcessed: 200, totalRecords: 1000,
            phaseStartedAtUtc: _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-1));
        analysisStats!.CurrentRpm.Should().Be(0, "snapshots were cleared");

        var deactivationStats = _sut.CalculatePhaseStats("op-1", "Deactivation",
            recordsProcessed: 100, totalRecords: 200,
            phaseStartedAtUtc: _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-1));
        deactivationStats!.CurrentRpm.Should().Be(0, "snapshots were cleared");
    }

    [Fact]
    public void ClearSnapshots_ShouldNotAffectOtherOperationPhases()
    {
        _sut.RecordSnapshot("op-1", "Analysis", 100);
        _sut.RecordSnapshot("op-2", "Analysis", 100);
        _timeProvider.Advance(TimeSpan.FromSeconds(10));
        _sut.RecordSnapshot("op-2", "Analysis", 200);

        _sut.ClearSnapshots("op-1");

        var stats = _sut.CalculatePhaseStats("op-2", "Analysis",
            recordsProcessed: 200, totalRecords: 1000,
            phaseStartedAtUtc: _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-1));
        stats!.CurrentRpm.Should().BeGreaterThan(0);
    }

    [Fact]
    public void LegacyRecordSnapshot_ShouldDelegateToAnalysisPhase()
    {
        // The legacy overload should work via the phase-keyed system
        _sut.RecordSnapshot("op-1", 100);
        _timeProvider.Advance(TimeSpan.FromSeconds(10));
        _sut.RecordSnapshot("op-1", 200);

        var phaseStats = _sut.CalculatePhaseStats("op-1", "Analysis",
            recordsProcessed: 200, totalRecords: 1000,
            phaseStartedAtUtc: _timeProvider.GetUtcNow().UtcDateTime.AddMinutes(-1));

        phaseStats!.CurrentRpm.Should().BeGreaterThan(0);
    }

    #endregion
}
