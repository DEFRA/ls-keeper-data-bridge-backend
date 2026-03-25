using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Operations.Command;
using KeeperData.Core.Reports.Cleanse.Operations.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Command.AggregateRoots;
using Moq;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Operations.Command;

public class OperationProgressTrackerTests
{
    private readonly Mock<ICleanseAnalysisOperationAggRootRepository> _repoMock = new();
    private readonly OperationProgressTracker _sut;

    public OperationProgressTrackerTests()
    {
        _sut = new OperationProgressTracker(_repoMock.Object);
    }

    private CleanseAnalysisOperation CreateOperation()
    {
        return CleanseAnalysisOperation.Create(100);
    }

    private async Task InitializeTrackerAsync(CleanseAnalysisOperation? operation = null)
    {
        operation ??= CreateOperation();
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(operation);
        await _sut.InitializeAsync(operation.Id);
    }

    #region InitializeAsync

    [Fact]
    public async Task InitializeAsync_ShouldLoadOperation()
    {
        var operation = CreateOperation();
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(operation);

        await _sut.InitializeAsync(operation.Id);

        _repoMock.Verify(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task InitializeAsync_ShouldThrow_WhenOperationNotFound()
    {
        _repoMock.Setup(r => r.GetByIdAsync("missing", It.IsAny<CancellationToken>()))
            .ReturnsAsync((CleanseAnalysisOperation?)null);

        var act = () => _sut.InitializeAsync("missing");

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*missing*not found*");
    }

    #endregion

    #region UpdateProgress

    [Fact]
    public async Task UpdateProgress_ShouldSetFieldsInMemory()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);

        _sut.UpdateProgress(50.0, "Halfway there", 250, 500, 10, 3);

        operation.ProgressPercentage.Should().Be(50.0);
        operation.StatusDescription.Should().Be("Halfway there");
        operation.RecordsAnalyzed.Should().Be(250);
        operation.TotalRecords.Should().Be(500);
        operation.IssuesFound.Should().Be(10);
        operation.IssuesResolved.Should().Be(3);
    }

    [Fact]
    public async Task UpdateProgress_ShouldNotWriteToDatabase()
    {
        await InitializeTrackerAsync();

        _sut.UpdateProgress(50.0, "test", 10, 100, 0, 0);

        _repoMock.Verify(r => r.UpdateAsync(It.IsAny<CleanseAnalysisOperation>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    #endregion

    #region StartPhase

    [Fact]
    public async Task StartPhase_ShouldMarkPhaseRunning()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);

        _sut.StartPhase(OperationPhase.Analysis, 500);

        var phase = operation.Phases.Find(p => p.Name == "Analysis");
        phase.Should().NotBeNull();
        phase!.Status.Should().Be("Running");
        phase.TotalRecords.Should().Be(500);
        operation.CurrentPhase.Should().Be("Analysis");
    }

    #endregion

    #region UpdatePhaseProgress

    [Fact]
    public async Task UpdatePhaseProgress_ShouldUpdatePhaseCounters()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);
        _sut.StartPhase(OperationPhase.Analysis, 1000);

        _sut.UpdatePhaseProgress(OperationPhase.Analysis, 250, 1000, "Analyzed 250 of 1000 records");

        var phase = operation.Phases.Find(p => p.Name == "Analysis");
        phase!.RecordsProcessed.Should().Be(250);
        phase.TotalRecords.Should().Be(1000);
        phase.Percentage.Should().Be(25.0);
        phase.Description.Should().Be("Analyzed 250 of 1000 records");
    }

    #endregion

    #region CompletePhase

    [Fact]
    public async Task CompletePhase_ShouldMarkPhaseCompleted()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);
        _sut.StartPhase(OperationPhase.Analysis, 100);

        _sut.CompletePhase(OperationPhase.Analysis);

        var phase = operation.Phases.Find(p => p.Name == "Analysis");
        phase!.Status.Should().Be("Completed");
        phase.Percentage.Should().Be(100.0);
        phase.CompletedAtUtc.Should().NotBeNull();
    }

    #endregion

    #region UpdateTimings

    [Fact]
    public async Task UpdateTimings_ShouldReplaceTimingTree()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);
        var timingNode = new TimingNode { Name = "Analysis", ElapsedMs = 5000 };

        _sut.UpdateTimings(timingNode);

        operation.Timings.Should().BeSameAs(timingNode);
    }

    #endregion

    #region FlushAsync — dirty path

    [Fact]
    public async Task FlushAsync_WhenDirty_ShouldPersistToDatabase()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);

        // Return a separate DB copy to verify snapshot is applied
        var dbCopy = CleanseAnalysisOperation.Create();
        dbCopy.Id = operation.Id; // same Id
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(dbCopy);

        _sut.UpdateProgress(42.0, "In progress", 42, 100, 5, 1);
        await _sut.FlushAsync(CancellationToken.None);

        _repoMock.Verify(r => r.UpdateAsync(
            It.Is<CleanseAnalysisOperation>(o =>
                o.ProgressPercentage == 42.0 &&
                o.StatusDescription == "In progress" &&
                o.RecordsAnalyzed == 42 &&
                o.TotalRecords == 100 &&
                o.IssuesFound == 5 &&
                o.IssuesResolved == 1),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task FlushAsync_WhenDirty_ShouldApplyPhaseProgressToDbCopy()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);
        _sut.StartPhase(OperationPhase.Deactivation, 50);
        _sut.UpdatePhaseProgress(OperationPhase.Deactivation, 25, 50, "Half done");

        var dbCopy = CleanseAnalysisOperation.Create();
        dbCopy.Id = operation.Id;
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(dbCopy);

        await _sut.FlushAsync(CancellationToken.None);

        _repoMock.Verify(r => r.UpdateAsync(
            It.Is<CleanseAnalysisOperation>(o =>
                o.Phases.Exists(p => p.Name == "Deactivation" && p.RecordsProcessed == 25 && p.Status == "Running")),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task FlushAsync_WhenDirty_ShouldRefreshCancellationFlag()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);

        var dbCopy = CleanseAnalysisOperation.Create();
        dbCopy.Id = operation.Id;
        dbCopy.CancellationRequested = true;
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(dbCopy);

        _sut.UpdateProgress(10, "test", 0, 0, 0, 0); // make dirty
        await _sut.FlushAsync(CancellationToken.None);

        _sut.IsCancellationRequested.Should().BeTrue();
    }

    [Fact]
    public async Task FlushAsync_WhenDirty_ShouldResetDirtyFlag()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);

        var dbCopy = CleanseAnalysisOperation.Create();
        dbCopy.Id = operation.Id;
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(dbCopy);

        _sut.UpdateProgress(10, "test", 0, 0, 0, 0);
        await _sut.FlushAsync(CancellationToken.None);

        // Second flush should NOT write (not dirty anymore)
        _repoMock.Invocations.Clear();
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(dbCopy);
        await _sut.FlushAsync(CancellationToken.None);

        _repoMock.Verify(r => r.UpdateAsync(It.IsAny<CleanseAnalysisOperation>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    #endregion

    #region FlushAsync — not dirty path

    [Fact]
    public async Task FlushAsync_WhenNotDirty_ShouldNotWriteToDatabase()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);

        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(operation);

        await _sut.FlushAsync(CancellationToken.None);

        _repoMock.Verify(r => r.UpdateAsync(It.IsAny<CleanseAnalysisOperation>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task FlushAsync_WhenNotDirty_ShouldStillRefreshCancellationFlag()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);

        var dbOp = CleanseAnalysisOperation.Create();
        dbOp.Id = operation.Id;
        dbOp.CancellationRequested = true;
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(dbOp);

        await _sut.FlushAsync(CancellationToken.None);

        _sut.IsCancellationRequested.Should().BeTrue();
    }

    [Fact]
    public async Task FlushAsync_WhenNotDirty_AndOperationDeletedFromDb_ShouldSetCancellationFalse()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);

        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync((CleanseAnalysisOperation?)null);

        await _sut.FlushAsync(CancellationToken.None);

        _sut.IsCancellationRequested.Should().BeFalse();
    }

    #endregion

    #region FlushAsync — edge cases

    [Fact]
    public async Task FlushAsync_WhenDirty_AndOperationDeletedFromDb_ShouldFallbackToRefreshCancellation()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);

        _sut.UpdateProgress(10, "test", 0, 0, 0, 0); // make dirty

        // First call (dirty path) returns null — operation was deleted
        // Second call (fallback path) returns null too
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync((CleanseAnalysisOperation?)null);

        await _sut.FlushAsync(CancellationToken.None);

        _repoMock.Verify(r => r.UpdateAsync(It.IsAny<CleanseAnalysisOperation>(), It.IsAny<CancellationToken>()),
            Times.Never);
        _sut.IsCancellationRequested.Should().BeFalse();
    }

    [Fact]
    public async Task FlushAsync_ShouldApplyTimingsToDbCopy()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);

        var timings = new TimingNode { Name = "Analysis", ElapsedMs = 12345 };
        _sut.UpdateTimings(timings);

        var dbCopy = CleanseAnalysisOperation.Create();
        dbCopy.Id = operation.Id;
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(dbCopy);

        await _sut.FlushAsync(CancellationToken.None);

        _repoMock.Verify(r => r.UpdateAsync(
            It.Is<CleanseAnalysisOperation>(o => o.Timings != null && o.Timings.Name == "Analysis" && o.Timings.ElapsedMs == 12345),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task FlushAsync_ShouldApplyCurrentPhaseToDbCopy()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);

        _sut.StartPhase(OperationPhase.Export, 0);

        var dbCopy = CleanseAnalysisOperation.Create();
        dbCopy.Id = operation.Id;
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(dbCopy);

        await _sut.FlushAsync(CancellationToken.None);

        _repoMock.Verify(r => r.UpdateAsync(
            It.Is<CleanseAnalysisOperation>(o => o.CurrentPhase == "Export"),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    #endregion

    #region IsCancellationRequested

    [Fact]
    public async Task IsCancellationRequested_ShouldBeFalseByDefault()
    {
        await InitializeTrackerAsync();
        _sut.IsCancellationRequested.Should().BeFalse();
    }

    [Fact]
    public async Task IsCancellationRequested_ShouldReflectDbStateAfterFlush()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);

        // First flush: not cancelled
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(operation);
        await _sut.FlushAsync(CancellationToken.None);
        _sut.IsCancellationRequested.Should().BeFalse();

        // Second flush: now cancelled
        var cancelled = CleanseAnalysisOperation.Create();
        cancelled.Id = operation.Id;
        cancelled.CancellationRequested = true;
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(cancelled);
        await _sut.FlushAsync(CancellationToken.None);
        _sut.IsCancellationRequested.Should().BeTrue();
    }

    #endregion

    #region RunPeriodicFlushAsync

    [Fact]
    public async Task RunPeriodicFlushAsync_ShouldExitWhenCancelled()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);

        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(operation);

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Should not throw — PeriodicTimer exits cleanly on cancellation
        var act = () => _sut.RunPeriodicFlushAsync(null, cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task RunPeriodicFlushAsync_WithTimings_ShouldCaptureTimingSnapshotBeforeFlush()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);

        var dbCopy = CleanseAnalysisOperation.Create();
        dbCopy.Id = operation.Id;
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(dbCopy);

        var timingTree = new TimingTree();
        timingTree.Track("CTS Pump/fetching", 100);

        using var cts = new CancellationTokenSource();

        // Let it tick once, then cancel.  The flush interval is 2s — we wait long enough for one tick.
        var trackerTask = _sut.RunPeriodicFlushAsync(timingTree, cts.Token);
        await Task.Delay(2500);
        await cts.CancelAsync();
        try { await trackerTask; } catch (OperationCanceledException) { }

        // Should have persisted timings from the timing tree
        _repoMock.Verify(r => r.UpdateAsync(
            It.Is<CleanseAnalysisOperation>(o => o.Timings != null && o.Timings.Name == "Analysis"),
            It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }

    #endregion

    #region Full lifecycle integration

    [Fact]
    public async Task FullLifecycle_ShouldTrackProgressAcrossPhases()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);

        // Analysis phase
        _sut.StartPhase(OperationPhase.Analysis, 1000);
        _sut.UpdatePhaseProgress(OperationPhase.Analysis, 500, 1000, "Halfway");
        _sut.UpdateProgress(0, "Halfway", 500, 1000, 5, 0);
        _sut.CompletePhase(OperationPhase.Analysis);

        // Deactivation phase
        _sut.StartPhase(OperationPhase.Deactivation, 20);
        _sut.UpdatePhaseProgress(OperationPhase.Deactivation, 20, 20, "Done");
        _sut.CompletePhase(OperationPhase.Deactivation);

        // Export phase
        _sut.StartPhase(OperationPhase.Export, 100);
        _sut.CompletePhase(OperationPhase.Export);

        // Verify aggregate progress computed correctly (all 3 phases 100%)
        operation.ProgressPercentage.Should().Be(100.0);

        // Flush everything
        var dbCopy = CleanseAnalysisOperation.Create();
        dbCopy.Id = operation.Id;
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(dbCopy);

        await _sut.FlushAsync(CancellationToken.None);

        _repoMock.Verify(r => r.UpdateAsync(
            It.Is<CleanseAnalysisOperation>(o =>
                o.ProgressPercentage == 100.0 &&
                o.Phases.TrueForAll(p => p.Status == "Completed")),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task MultipleMutationsBeforeFlush_ShouldBatchIntoSingleWrite()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);

        // Many rapid mutations — all in memory
        for (var i = 1; i <= 10; i++)
        {
            _sut.UpdateProgress(i * 10, $"Step {i}", i * 10, 100, i, 0);
        }

        var dbCopy = CleanseAnalysisOperation.Create();
        dbCopy.Id = operation.Id;
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(dbCopy);

        await _sut.FlushAsync(CancellationToken.None);

        // Only the final state should be persisted — single write
        _repoMock.Verify(r => r.UpdateAsync(
            It.Is<CleanseAnalysisOperation>(o =>
                o.ProgressPercentage == 100.0 &&
                o.StatusDescription == "Step 10" &&
                o.RecordsAnalyzed == 100),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    #endregion
}
