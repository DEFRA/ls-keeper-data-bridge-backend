using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Operations.Command;
using KeeperData.Core.Reports.Cleanse.Operations.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Command.AggregateRoots;
using KeeperData.Core.Reports.Operations;
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

        _sut.UpdateProgress(new OperationNode
        {
            Name = "Test",
            PercentComplete = 50.0,
            Description = "Halfway there",
            ProcessedCount = 250,
            TotalRecords = 500
        });

        operation.Progress.Should().NotBeNull();
        operation.Progress!.Name.Should().Be("Test");
        operation.Progress.PercentComplete.Should().Be(50.0);
        operation.Progress.Description.Should().Be("Halfway there");
        operation.Progress.ProcessedCount.Should().Be(250);
        operation.Progress.TotalRecords.Should().Be(500);
    }

    [Fact]
    public async Task UpdateProgress_ShouldNotWriteToDatabase()
    {
        await InitializeTrackerAsync();

        _sut.UpdateProgress(new OperationNode { Name = "Test" });

        _repoMock.Verify(r => r.UpdateAsync(It.IsAny<CleanseAnalysisOperation>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    #endregion

    #region FlushAsync — dirty path

    [Fact]
    public async Task FlushAsync_WhenDirty_ShouldPersistToDatabase()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);

        var dbCopy = CleanseAnalysisOperation.Create();
        dbCopy.Id = operation.Id;
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(dbCopy);

        _sut.UpdateProgress(new OperationNode { Name = "Test", PercentComplete = 42.0 });
        await _sut.FlushAsync(CancellationToken.None);

        _repoMock.Verify(r => r.UpdateAsync(
            It.Is<CleanseAnalysisOperation>(o =>
                o.Progress != null &&
                o.Progress.PercentComplete == 42.0),
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

        _sut.UpdateProgress(new OperationNode { Name = "Test" }); // make dirty
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

        _sut.UpdateProgress(new OperationNode { Name = "Test" });
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

        _sut.UpdateProgress(new OperationNode { Name = "Test" }); // make dirty

        // First call (dirty path) returns null — operation was deleted
        // Second call (fallback path) returns null too
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync((CleanseAnalysisOperation?)null);

        await _sut.FlushAsync(CancellationToken.None);

        _repoMock.Verify(r => r.UpdateAsync(It.IsAny<CleanseAnalysisOperation>(), It.IsAny<CancellationToken>()),
            Times.Never);
        _sut.IsCancellationRequested.Should().BeFalse();
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

        var act = () => _sut.RunPeriodicFlushAsync(cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    #endregion

    #region MultipleMutationsBeforeFlush

    [Fact]
    public async Task MultipleMutationsBeforeFlush_ShouldBatchIntoSingleWrite()
    {
        var operation = CreateOperation();
        await InitializeTrackerAsync(operation);

        for (var i = 1; i <= 10; i++)
        {
            _sut.UpdateProgress(new OperationNode { Name = "Test", PercentComplete = i * 10 });
        }

        var dbCopy = CleanseAnalysisOperation.Create();
        dbCopy.Id = operation.Id;
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(dbCopy);

        await _sut.FlushAsync(CancellationToken.None);

        _repoMock.Verify(r => r.UpdateAsync(
            It.Is<CleanseAnalysisOperation>(o =>
                o.Progress != null &&
                o.Progress.PercentComplete == 100),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    #endregion
}
