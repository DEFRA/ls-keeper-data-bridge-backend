using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Operations.Command;
using KeeperData.Core.Reports.Cleanse.Operations.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Operations.Command.AggregateRoots;
using KeeperData.Core.Reports.Cleanse.Operations.Command.Requests;
using Moq;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Operations.Command;

public class CleanseOperationCommandServiceTests
{
    private readonly Mock<ICleanseAnalysisOperationAggRootRepository> _repoMock = new();
    private readonly CleanseOperationCommandService _sut;

    public CleanseOperationCommandServiceTests()
    {
        _sut = new CleanseOperationCommandService(_repoMock.Object);
    }

    [Fact]
    public async Task CreateOperationAsync_ShouldPersistAndReturnId()
    {
        var result = await _sut.CreateOperationAsync(new CreateOperationCommand(), CancellationToken.None);

        result.Should().NotBeNullOrEmpty();
        _repoMock.Verify(r => r.CreateAsync(It.Is<CleanseAnalysisOperation>(o => o.Id == result), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task UpdateProgressAsync_ShouldUpdateAndPersist()
    {
        var operation = CleanseAnalysisOperation.Create();
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>())).ReturnsAsync(operation);

        await _sut.UpdateProgressAsync(
            new UpdateProgressCommand(operation.Id, 50.0, "Halfway", 100, 500, 5, 0), CancellationToken.None);

        operation.ProgressPercentage.Should().Be(50.0);
        operation.RecordsAnalyzed.Should().Be(100);
        operation.TotalRecords.Should().Be(500);
        operation.IssuesFound.Should().Be(5);
        _repoMock.Verify(r => r.UpdateAsync(operation, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CompleteOperationAsync_ShouldMarkCompletedAndPersist()
    {
        var operation = CleanseAnalysisOperation.Create();
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>())).ReturnsAsync(operation);

        await _sut.CompleteOperationAsync(
            new CompleteOperationCommand(operation.Id, 200, 10, 2, 5000), CancellationToken.None);

        operation.Status.Should().Be(CleanseAnalysisStatus.Completed);
        operation.RecordsAnalyzed.Should().Be(200);
        operation.IssuesFound.Should().Be(10);
        operation.IssuesResolved.Should().Be(2);
        operation.DurationMs.Should().Be(5000);
        _repoMock.Verify(r => r.UpdateAsync(operation, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task FailOperationAsync_ShouldMarkFailedAndPersist()
    {
        var operation = CleanseAnalysisOperation.Create();
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>())).ReturnsAsync(operation);

        await _sut.FailOperationAsync(
            new FailOperationCommand(operation.Id, "Something broke", 1000), CancellationToken.None);

        operation.Status.Should().Be(CleanseAnalysisStatus.Failed);
        operation.Error.Should().Be("Something broke");
        operation.DurationMs.Should().Be(1000);
    }

    [Fact]
    public async Task SetReportDetailsAsync_ShouldSetReportFieldsAndPersist()
    {
        var operation = CleanseAnalysisOperation.Create();
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>())).ReturnsAsync(operation);

        await _sut.SetReportDetailsAsync(
            new SetReportDetailsCommand(operation.Id, "key.zip", "https://url"), CancellationToken.None);

        operation.ReportObjectKey.Should().Be("key.zip");
        operation.ReportUrl.Should().Be("https://url");
    }

    [Fact]
    public async Task UpdateReportUrlAsync_ShouldUpdateUrlAndPersist()
    {
        var operation = CleanseAnalysisOperation.Create();
        operation.SetReportDetails("key.zip", "https://old-url");
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>())).ReturnsAsync(operation);

        await _sut.UpdateReportUrlAsync(
            new UpdateReportUrlCommand(operation.Id, "https://new-url"), CancellationToken.None);

        operation.ReportUrl.Should().Be("https://new-url");
        operation.ReportObjectKey.Should().Be("key.zip");
    }

    [Fact]
    public async Task UpdateProgressAsync_WhenNotFound_ShouldThrow()
    {
        _repoMock.Setup(r => r.GetByIdAsync("missing", It.IsAny<CancellationToken>())).ReturnsAsync((CleanseAnalysisOperation?)null);

        var act = () => _sut.UpdateProgressAsync(
            new UpdateProgressCommand("missing", 0, "", 0, 0, 0, 0), CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*not found*");
    }

    [Fact]
    public async Task RequestCancellationAsync_ShouldSetFlagAndPersist()
    {
        var operation = CleanseAnalysisOperation.Create();
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>())).ReturnsAsync(operation);

        await _sut.RequestCancellationAsync(
            new CancelOperationCommand(operation.Id), CancellationToken.None);

        operation.CancellationRequested.Should().BeTrue();
        _repoMock.Verify(r => r.UpdateAsync(operation, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CancelOperationAsync_ShouldMarkCancelledAndPersist()
    {
        var operation = CleanseAnalysisOperation.Create();
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>())).ReturnsAsync(operation);

        await _sut.CancelOperationAsync(
            new CancelOperationCommand(operation.Id), 5000, CancellationToken.None);

        operation.Status.Should().Be(CleanseAnalysisStatus.Cancelled);
        operation.DurationMs.Should().Be(5000);
        operation.CancelledAtUtc.Should().NotBeNull();
        _repoMock.Verify(r => r.UpdateAsync(operation, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task IsCancellationRequestedAsync_WhenNotRequested_ShouldReturnFalse()
    {
        var operation = CleanseAnalysisOperation.Create();
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>())).ReturnsAsync(operation);

        var result = await _sut.IsCancellationRequestedAsync(operation.Id, CancellationToken.None);

        result.Should().BeFalse();
    }

    [Fact]
    public async Task IsCancellationRequestedAsync_WhenRequested_ShouldReturnTrue()
    {
        var operation = CleanseAnalysisOperation.Create();
        operation.RequestCancellation();
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>())).ReturnsAsync(operation);

        var result = await _sut.IsCancellationRequestedAsync(operation.Id, CancellationToken.None);

        result.Should().BeTrue();
    }

    [Fact]
    public async Task IsCancellationRequestedAsync_WhenNotFound_ShouldReturnFalse()
    {
        _repoMock.Setup(r => r.GetByIdAsync("missing", It.IsAny<CancellationToken>())).ReturnsAsync((CleanseAnalysisOperation?)null);

        var result = await _sut.IsCancellationRequestedAsync("missing", CancellationToken.None);

        result.Should().BeFalse();
    }

    [Fact]
    public async Task RequestCancellationAsync_WhenNotFound_ShouldThrow()
    {
        _repoMock.Setup(r => r.GetByIdAsync("missing", It.IsAny<CancellationToken>())).ReturnsAsync((CleanseAnalysisOperation?)null);

        var act = () => _sut.RequestCancellationAsync(
            new CancelOperationCommand("missing"), CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*not found*");
    }

    #region Phase commands

    [Fact]
    public async Task StartPhaseAsync_ShouldSetPhaseRunningAndPersist()
    {
        var operation = CleanseAnalysisOperation.Create();
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>())).ReturnsAsync(operation);

        await _sut.StartPhaseAsync(
            new StartPhaseCommand(operation.Id, OperationPhase.Analysis, 500), CancellationToken.None);

        var phase = operation.Phases.Single(p => p.Name == "Analysis");
        phase.Status.Should().Be("Running");
        phase.TotalRecords.Should().Be(500);
        operation.CurrentPhase.Should().Be("Analysis");
        _repoMock.Verify(r => r.UpdateAsync(operation, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task UpdatePhaseProgressAsync_ShouldUpdatePhaseAndPersist()
    {
        var operation = CleanseAnalysisOperation.Create();
        operation.StartPhase(OperationPhase.Deactivation, 100);
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>())).ReturnsAsync(operation);

        await _sut.UpdatePhaseProgressAsync(
            new UpdatePhaseProgressCommand(operation.Id, OperationPhase.Deactivation, 50, 100, "Half done"),
            CancellationToken.None);

        var phase = operation.Phases.Single(p => p.Name == "Deactivation");
        phase.RecordsProcessed.Should().Be(50);
        phase.Percentage.Should().Be(50.0);
        _repoMock.Verify(r => r.UpdateAsync(operation, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CompletePhaseAsync_ShouldSetPhaseCompletedAndPersist()
    {
        var operation = CleanseAnalysisOperation.Create();
        operation.StartPhase(OperationPhase.Export, 200);
        _repoMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>())).ReturnsAsync(operation);

        await _sut.CompletePhaseAsync(
            new CompletePhaseCommand(operation.Id, OperationPhase.Export), CancellationToken.None);

        var phase = operation.Phases.Single(p => p.Name == "Export");
        phase.Status.Should().Be("Completed");
        phase.Percentage.Should().Be(100.0);
        phase.CompletedAtUtc.Should().NotBeNull();
        _repoMock.Verify(r => r.UpdateAsync(operation, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StartPhaseAsync_WhenNotFound_ShouldThrow()
    {
        _repoMock.Setup(r => r.GetByIdAsync("missing", It.IsAny<CancellationToken>())).ReturnsAsync((CleanseAnalysisOperation?)null);

        var act = () => _sut.StartPhaseAsync(
            new StartPhaseCommand("missing", OperationPhase.Analysis, 0), CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*not found*");
    }

    #endregion
}
