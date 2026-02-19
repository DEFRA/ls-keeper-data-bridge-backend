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
            new UpdateProgressCommand(operation.Id, 50.0, "Halfway", 100, 5, 0), CancellationToken.None);

        operation.ProgressPercentage.Should().Be(50.0);
        operation.RecordsAnalyzed.Should().Be(100);
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
            new UpdateProgressCommand("missing", 0, "", 0, 0, 0), CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*not found*");
    }
}
