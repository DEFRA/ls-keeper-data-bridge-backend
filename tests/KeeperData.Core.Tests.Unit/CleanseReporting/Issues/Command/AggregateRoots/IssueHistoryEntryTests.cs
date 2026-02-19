using FluentAssertions;
using KeeperData.Core.Reports.Issues.Command.AggregateRoots;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Issues.Command.AggregateRoots;

public class IssueHistoryEntryTests
{
    [Fact]
    public void Create_ShouldPopulateAllFields()
    {
        var before = DateTime.UtcNow;

        var entry = IssueHistoryEntry.Create("issue-1", IssueAction.Created, "system", "Issue detected");

        entry.Id.Should().NotBeNullOrEmpty();
        entry.IssueId.Should().Be("issue-1");
        entry.Action.Should().Be(IssueAction.Created);
        entry.PerformedBy.Should().Be("system");
        entry.Detail.Should().Be("Issue detected");
        entry.OccurredAtUtc.Should().BeOnOrAfter(before).And.BeOnOrBefore(DateTime.UtcNow);
    }

    [Fact]
    public void Create_WithNullDetail_ShouldLeaveDetailNull()
    {
        var entry = IssueHistoryEntry.Create("issue-2", IssueAction.Ignored, "user@example.com");

        entry.Detail.Should().BeNull();
    }

    [Fact]
    public void Create_ShouldGenerateUniqueIds()
    {
        var entry1 = IssueHistoryEntry.Create("issue-1", IssueAction.Created, "system");
        var entry2 = IssueHistoryEntry.Create("issue-1", IssueAction.Created, "system");

        entry1.Id.Should().NotBe(entry2.Id);
    }

    [Theory]
    [InlineData(IssueAction.Created)]
    [InlineData(IssueAction.Reactivated)]
    [InlineData(IssueAction.Deactivated)]
    [InlineData(IssueAction.Touched)]
    [InlineData(IssueAction.Ignored)]
    [InlineData(IssueAction.Unignored)]
    [InlineData(IssueAction.ResolutionStatusChanged)]
    [InlineData(IssueAction.Assigned)]
    [InlineData(IssueAction.Unassigned)]
    public void Create_ShouldPreserveAction(IssueAction action)
    {
        var entry = IssueHistoryEntry.Create("issue-1", action, "system");

        entry.Action.Should().Be(action);
    }
}
