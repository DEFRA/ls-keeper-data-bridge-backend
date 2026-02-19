using FluentAssertions;
using KeeperData.Core.Reports.Cleanse.Analysis.Command.Domain;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Issues.Command.AggregateRoots;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Issues.Command.AggregateRoots;

public class IssueTests
{
    private static readonly Cph TestCph = Cph.Parse("12/345/6789");
    private static readonly RuleDescriptor TestDescriptor = new("TEST_RULE", "1", "01", "Test description", "ULITP-0000");

    [Fact]
    public void Create_ShouldReturnActiveIssueAndCreatedHistory()
    {
        var (issue, history) = Issue.Create("thumb-1", "op-1", TestDescriptor, TestCph, "UK-12/345/6789");

        issue.Id.Should().Be("thumb-1");
        issue.OperationId.Should().Be("op-1");
        issue.IssueCode.Should().Be("TEST_RULE");
        issue.RuleCode.Should().Be("1");
        issue.ErrorCode.Should().Be("01");
        issue.ErrorDescription.Should().Be("Test description");
        issue.Cph.Should().Be("12/345/6789");
        issue.CtsLidFullIdentifier.Should().Be("UK-12/345/6789");
        issue.IsActive.Should().BeTrue();
        issue.IsIgnored.Should().BeFalse();

        history.IssueId.Should().Be("thumb-1");
        history.Action.Should().Be(IssueAction.Created);
        history.PerformedBy.Should().Be("system");
    }

    [Fact]
    public void Create_WithNullLidFullIdentifier_ShouldDefaultToEmpty()
    {
        var (issue, _) = Issue.Create("thumb-1", "op-1", TestDescriptor, TestCph);

        issue.CtsLidFullIdentifier.Should().BeEmpty();
    }

    [Fact]
    public void Create_WithContext_ShouldApplyContextFields()
    {
        var context = new IssueContextData
        {
            EmailCTS = ["a@test.com"],
            TelCTS = ["01234567890"],
            FSA = "fsa-1"
        };

        var (issue, _) = Issue.Create("thumb-1", "op-1", TestDescriptor, TestCph, context: context);

        issue.EmailCTS.Should().BeEquivalentTo(["a@test.com"]);
        issue.TelCTS.Should().BeEquivalentTo(["01234567890"]);
        issue.FSA.Should().Be("fsa-1");
    }

    [Fact]
    public void Reactivate_ShouldSetActiveAndReturnReactivatedHistory()
    {
        var (issue, _) = Issue.Create("thumb-1", "op-1", TestDescriptor, TestCph);
        _ = issue.Deactivate();
        issue.IsActive.Should().BeFalse();

        var history = issue.Reactivate("op-2");

        issue.IsActive.Should().BeTrue();
        issue.OperationId.Should().Be("op-2");
        history.Action.Should().Be(IssueAction.Reactivated);
    }

    [Fact]
    public void Touch_ShouldUpdateOperationIdAndReturnTouchedHistory()
    {
        var (issue, _) = Issue.Create("thumb-1", "op-1", TestDescriptor, TestCph);

        var history = issue.Touch("op-2");

        issue.OperationId.Should().Be("op-2");
        history.Action.Should().Be(IssueAction.Touched);
    }

    [Fact]
    public void Deactivate_ShouldSetInactiveAndReturnDeactivatedHistory()
    {
        var (issue, _) = Issue.Create("thumb-1", "op-1", TestDescriptor, TestCph);

        var history = issue.Deactivate();

        issue.IsActive.Should().BeFalse();
        history.Action.Should().Be(IssueAction.Deactivated);
    }

    [Fact]
    public void Ignore_ShouldSetIgnoredAndReturnHistory()
    {
        var (issue, _) = Issue.Create("thumb-1", "op-1", TestDescriptor, TestCph);

        var history = issue.Ignore("user@test.com");

        issue.IsIgnored.Should().BeTrue();
        history.Action.Should().Be(IssueAction.Ignored);
        history.PerformedBy.Should().Be("user@test.com");
    }

    [Fact]
    public void Unignore_ShouldClearIgnoredAndReturnHistory()
    {
        var (issue, _) = Issue.Create("thumb-1", "op-1", TestDescriptor, TestCph);
        issue.Ignore("user@test.com");

        var history = issue.Unignore("admin@test.com");

        issue.IsIgnored.Should().BeFalse();
        history.Action.Should().Be(IssueAction.Unignored);
        history.PerformedBy.Should().Be("admin@test.com");
    }

    [Fact]
    public void UpdateResolutionStatus_ShouldChangeStatusAndRecordPreviousInHistory()
    {
        var (issue, _) = Issue.Create("thumb-1", "op-1", TestDescriptor, TestCph);

        var history = issue.UpdateResolutionStatus(ResolutionStatus.InProgress, "user@test.com");

        issue.ResolutionStatus.Should().Be(ResolutionStatus.InProgress);
        history.Action.Should().Be(IssueAction.ResolutionStatusChanged);
        history.Detail.Should().Contain("None").And.Contain("InProgress");
    }

    [Fact]
    public void Assign_ShouldSetAssigneeAndReturnHistory()
    {
        var (issue, _) = Issue.Create("thumb-1", "op-1", TestDescriptor, TestCph);

        var history = issue.Assign("dev@test.com", "admin@test.com");

        issue.AssignedTo.Should().Be("dev@test.com");
        history.Action.Should().Be(IssueAction.Assigned);
        history.Detail.Should().Contain("dev@test.com");
    }

    [Fact]
    public void Unassign_ShouldClearAssigneeAndReturnHistory()
    {
        var (issue, _) = Issue.Create("thumb-1", "op-1", TestDescriptor, TestCph);
        issue.Assign("dev@test.com", "admin");

        var history = issue.Unassign("admin@test.com");

        issue.AssignedTo.Should().BeNull();
        history.Action.Should().Be(IssueAction.Unassigned);
        history.Detail.Should().Contain("dev@test.com");
    }

    [Fact]
    public void ApplyContext_WithNull_ShouldNotChangeFields()
    {
        var (issue, _) = Issue.Create("thumb-1", "op-1", TestDescriptor, TestCph);

        issue.ApplyContext(null);

        issue.EmailCTS.Should().BeNull();
        issue.TelCTS.Should().BeNull();
        issue.FSA.Should().BeNull();
    }
}
