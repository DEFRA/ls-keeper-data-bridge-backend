using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Issues.Command.Abstract;
using KeeperData.Core.Reports.Issues.Command.AggregateRoots;
using KeeperData.Core.Reports.Issues.Command.Requests;
using KeeperData.Core.Reports.Issues.Command;
using Moq;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Issues.Command;

public class IssueCommandServiceTests
{
    private readonly Mock<IIssueAggRootRepository> _repoMock = new();
    private readonly Mock<IIssueHistoryAggRootRepository> _historyRepoMock = new();
    private readonly IssueCommandService _sut;

    private static readonly Cph TestCph = Cph.Parse("12/345/6789");
    private static readonly RuleDescriptor TestDescriptor = new("TEST", "1", "01", "desc", "TAG");

    public IssueCommandServiceTests()
    {
        _sut = new IssueCommandService(_repoMock.Object, _historyRepoMock.Object);
    }

    [Fact]
    public async Task RecordIssueAsync_WhenNew_ShouldCreateAndReturnCreated()
    {
        _repoMock.Setup(r => r.GetByIdAsync("thumb", It.IsAny<CancellationToken>()))
            .ReturnsAsync((Issue?)null);

        var command = new RecordIssueCommand("op-1", "thumb", TestDescriptor, TestCph);
        var result = await _sut.RecordIssueAsync(command, CancellationToken.None);

        result.Should().Be(IssueRecordResult.Created);
        _repoMock.Verify(r => r.UpsertAsync(It.Is<Issue>(i => i.Id == "thumb" && i.IsActive), It.IsAny<CancellationToken>()), Times.Once);
        _historyRepoMock.Verify(r => r.AppendAsync(It.Is<IssueHistoryEntry>(h => h.Action == IssueAction.Created), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task RecordIssueAsync_WhenExistingActive_ShouldTouchAndReturnNoChange()
    {
        var (existing, _) = Issue.Create("thumb", "op-old", TestDescriptor, TestCph);
        _repoMock.Setup(r => r.GetByIdAsync("thumb", It.IsAny<CancellationToken>()))
            .ReturnsAsync(existing);

        var command = new RecordIssueCommand("op-2", "thumb", TestDescriptor, TestCph);
        var result = await _sut.RecordIssueAsync(command, CancellationToken.None);

        result.Should().Be(IssueRecordResult.NoChange);
        existing.OperationId.Should().Be("op-2");
        _historyRepoMock.Verify(r => r.AppendAsync(It.Is<IssueHistoryEntry>(h => h.Action == IssueAction.Touched), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task RecordIssueAsync_WhenExistingInactive_ShouldReactivateAndReturnReactivated()
    {
        var (existing, _) = Issue.Create("thumb", "op-old", TestDescriptor, TestCph);
        existing.Deactivate();

        _repoMock.Setup(r => r.GetByIdAsync("thumb", It.IsAny<CancellationToken>()))
            .ReturnsAsync(existing);

        var command = new RecordIssueCommand("op-2", "thumb", TestDescriptor, TestCph);
        var result = await _sut.RecordIssueAsync(command, CancellationToken.None);

        result.Should().Be(IssueRecordResult.Reactivated);
        existing.IsActive.Should().BeTrue();
        _historyRepoMock.Verify(r => r.AppendAsync(It.Is<IssueHistoryEntry>(h => h.Action == IssueAction.Reactivated), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task DeactivateStaleIssuesAsync_ShouldDelegateToRepository()
    {
        _repoMock.Setup(r => r.DeactivateStaleAsync("op-1", It.IsAny<CancellationToken>()))
            .ReturnsAsync(3);

        var result = await _sut.DeactivateStaleIssuesAsync(new DeactivateStaleIssuesCommand("op-1"), CancellationToken.None);

        result.Should().Be(3);
    }

    [Fact]
    public async Task IgnoreIssueAsync_ShouldSetIgnoredAndPersist()
    {
        var (issue, _) = Issue.Create("id-1", "op-1", TestDescriptor, TestCph);
        _repoMock.Setup(r => r.GetByIdAsync("id-1", It.IsAny<CancellationToken>())).ReturnsAsync(issue);

        await _sut.IgnoreIssueAsync(new IgnoreIssueCommand("id-1", "user"), CancellationToken.None);

        issue.IsIgnored.Should().BeTrue();
        _repoMock.Verify(r => r.UpsertAsync(issue, It.IsAny<CancellationToken>()), Times.Once);
        _historyRepoMock.Verify(r => r.AppendAsync(It.Is<IssueHistoryEntry>(h => h.Action == IssueAction.Ignored), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task UnignoreIssueAsync_ShouldClearIgnoredAndPersist()
    {
        var (issue, _) = Issue.Create("id-1", "op-1", TestDescriptor, TestCph);
        issue.Ignore("user");
        _repoMock.Setup(r => r.GetByIdAsync("id-1", It.IsAny<CancellationToken>())).ReturnsAsync(issue);

        await _sut.UnignoreIssueAsync(new UnignoreIssueCommand("id-1", "user"), CancellationToken.None);

        issue.IsIgnored.Should().BeFalse();
    }

    [Fact]
    public async Task UpdateResolutionStatusAsync_ShouldChangeStatusAndPersist()
    {
        var (issue, _) = Issue.Create("id-1", "op-1", TestDescriptor, TestCph);
        _repoMock.Setup(r => r.GetByIdAsync("id-1", It.IsAny<CancellationToken>())).ReturnsAsync(issue);

        await _sut.UpdateResolutionStatusAsync(
            new UpdateResolutionStatusCommand("id-1", ResolutionStatus.InProgress, "user"), CancellationToken.None);

        issue.ResolutionStatus.Should().Be(ResolutionStatus.InProgress);
    }

    [Fact]
    public async Task AssignIssueAsync_ShouldSetAssigneeAndPersist()
    {
        var (issue, _) = Issue.Create("id-1", "op-1", TestDescriptor, TestCph);
        _repoMock.Setup(r => r.GetByIdAsync("id-1", It.IsAny<CancellationToken>())).ReturnsAsync(issue);

        await _sut.AssignIssueAsync(new AssignIssueCommand("id-1", "dev", "admin"), CancellationToken.None);

        issue.AssignedTo.Should().Be("dev");
    }

    [Fact]
    public async Task UnassignIssueAsync_ShouldClearAssigneeAndPersist()
    {
        var (issue, _) = Issue.Create("id-1", "op-1", TestDescriptor, TestCph);
        issue.Assign("dev", "admin");
        _repoMock.Setup(r => r.GetByIdAsync("id-1", It.IsAny<CancellationToken>())).ReturnsAsync(issue);

        await _sut.UnassignIssueAsync(new UnassignIssueCommand("id-1", "admin"), CancellationToken.None);

        issue.AssignedTo.Should().BeNull();
    }

    [Fact]
    public async Task IgnoreIssueAsync_WhenNotFound_ShouldThrow()
    {
        _repoMock.Setup(r => r.GetByIdAsync("missing", It.IsAny<CancellationToken>())).ReturnsAsync((Issue?)null);

        var act = () => _sut.IgnoreIssueAsync(new IgnoreIssueCommand("missing", "user"), CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*not found*");
    }

    [Fact]
    public async Task DeleteAllIssuesAsync_ShouldDelegateToRepository()
    {
        _repoMock.Setup(r => r.DeleteAllAsync(It.IsAny<CancellationToken>())).ReturnsAsync(5);

        var result = await _sut.DeleteAllIssuesAsync(CancellationToken.None);

        result.Should().Be(5);
    }
}
