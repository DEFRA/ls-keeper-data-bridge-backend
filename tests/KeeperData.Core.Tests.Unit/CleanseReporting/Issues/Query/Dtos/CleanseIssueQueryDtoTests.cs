using FluentAssertions;
using KeeperData.Core.Reports.Issues.Query.Dtos;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Issues.Query.Dtos;

public class CleanseIssueQueryDtoTests
{
    [Fact]
    public void Create_ShouldReturnDefaultQuery()
    {
        var query = CleanseIssueQueryDto.Create();

        query.IsActive.Should().BeNull();
        query.IssueCode.Should().BeNull();
        query.CphContains.Should().BeNull();
        query.Top.Should().Be(50);
        query.Skip.Should().Be(0);
        query.SortDescending.Should().BeTrue();
        query.SortBy.Should().Be(CleanseIssueSortField.LastUpdatedAtUtc);
    }

    [Fact]
    public void WhereActive_ShouldSetIsActiveTrue()
    {
        var query = CleanseIssueQueryDto.Create().WhereActive();

        query.IsActive.Should().BeTrue();
    }

    [Fact]
    public void WhereInactive_ShouldSetIsActiveFalse()
    {
        var query = CleanseIssueQueryDto.Create().WhereInactive();

        query.IsActive.Should().BeFalse();
    }

    [Fact]
    public void WithIssueCode_ShouldSetIssueCode()
    {
        var query = CleanseIssueQueryDto.Create().WithIssueCode("RULE_1");

        query.IssueCode.Should().Be("RULE_1");
    }

    [Fact]
    public void WithCphContaining_ShouldSetCphContains()
    {
        var query = CleanseIssueQueryDto.Create().WithCphContaining("12/345");

        query.CphContains.Should().Be("12/345");
    }

    [Fact]
    public void WithCphStartingWith_ShouldSetCphStartsWith()
    {
        var query = CleanseIssueQueryDto.Create().WithCphStartingWith("12/");

        query.CphStartsWith.Should().Be("12/");
    }

    [Fact]
    public void DateFilters_ShouldSetCorrectValues()
    {
        var after = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var before = new DateTime(2025, 6, 1, 0, 0, 0, DateTimeKind.Utc);

        var query = CleanseIssueQueryDto.Create()
            .CreatedAfter(after)
            .CreatedBefore(before)
            .UpdatedAfter(after)
            .UpdatedBefore(before);

        query.CreatedAfterUtc.Should().Be(after);
        query.CreatedBeforeUtc.Should().Be(before);
        query.UpdatedAfterUtc.Should().Be(after);
        query.UpdatedBeforeUtc.Should().Be(before);
    }

    [Fact]
    public void WhereIgnored_ShouldSetIsIgnoredTrue()
    {
        var query = CleanseIssueQueryDto.Create().WhereIgnored();

        query.IsIgnored.Should().BeTrue();
    }

    [Fact]
    public void WhereNotIgnored_ShouldSetIsIgnoredFalse()
    {
        var query = CleanseIssueQueryDto.Create().WhereNotIgnored();

        query.IsIgnored.Should().BeFalse();
    }

    [Fact]
    public void WithResolutionStatus_ShouldSetStatus()
    {
        var query = CleanseIssueQueryDto.Create().WithResolutionStatus("InProgress");

        query.ResolutionStatus.Should().Be("InProgress");
    }

    [Fact]
    public void WithAssignedTo_ShouldSetUser()
    {
        var query = CleanseIssueQueryDto.Create().WithAssignedTo("user@test.com");

        query.AssignedTo.Should().Be("user@test.com");
    }

    [Fact]
    public void WhereUnassigned_ShouldSetIsUnassignedTrue()
    {
        var query = CleanseIssueQueryDto.Create().WhereUnassigned();

        query.IsUnassigned.Should().BeTrue();
    }

    [Fact]
    public void OrderBy_ShouldSetSortFieldAndDirection()
    {
        var query = CleanseIssueQueryDto.Create().OrderBy(CleanseIssueSortField.Cph, descending: true);

        query.SortBy.Should().Be(CleanseIssueSortField.Cph);
        query.SortDescending.Should().BeTrue();
    }

    [Fact]
    public void Page_ShouldSetSkipAndTop()
    {
        var query = CleanseIssueQueryDto.Create().Page(20, 10);

        query.Skip.Should().Be(20);
        query.Top.Should().Be(10);
    }

    [Fact]
    public void FluentChaining_ShouldApplyAllFilters()
    {
        var query = CleanseIssueQueryDto.Create()
            .WhereActive()
            .WithIssueCode("RULE_2A")
            .WithCphContaining("12/345")
            .WhereNotIgnored()
            .OrderBy(CleanseIssueSortField.CreatedAtUtc)
            .Page(0, 25);

        query.IsActive.Should().BeTrue();
        query.IssueCode.Should().Be("RULE_2A");
        query.CphContains.Should().Be("12/345");
        query.IsIgnored.Should().BeFalse();
        query.SortBy.Should().Be(CleanseIssueSortField.CreatedAtUtc);
        query.Skip.Should().Be(0);
        query.Top.Should().Be(25);
    }

    [Fact]
    public void From_WithFilters_ShouldMapAllProperties()
    {
        var created = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var updated = new DateTime(2025, 6, 1, 0, 0, 0, DateTimeKind.Utc);

        var query = CleanseIssueQueryDto.From(
            CleanseIssueSortField.Cph, sortDescending: true, skip: 5, top: 20,
            new CleanseIssueFilterValues
            {
                IsActive = true,
                CtsLidFullIdentifier = "UK-12/345/6789",
                Cph = "12/345",
                IssueCode = "RULE_1",
                RuleCode = "2A",
                ErrorCode = "02A",
                IsIgnored = false,
                ResolutionStatus = "InProgress",
                AssignedTo = "user@test.com",
                IsUnassigned = true,
                CreatedAfterUtc = created,
                CreatedBeforeUtc = updated,
                UpdatedAfterUtc = created,
                UpdatedBeforeUtc = updated
            });

        query.SortBy.Should().Be(CleanseIssueSortField.Cph);
        query.SortDescending.Should().BeTrue();
        query.Skip.Should().Be(5);
        query.Top.Should().Be(20);
        query.IsActive.Should().BeTrue();
        query.CtsLidFullIdentifierContains.Should().Be("UK-12/345/6789");
        query.CphContains.Should().Be("12/345");
        query.IssueCode.Should().Be("RULE_1");
        query.RuleCode.Should().Be("2A");
        query.ErrorCode.Should().Be("02A");
        query.IsIgnored.Should().BeFalse();
        query.ResolutionStatus.Should().Be("InProgress");
        query.AssignedTo.Should().Be("user@test.com");
        query.IsUnassigned.Should().BeTrue();
        query.CreatedAfterUtc.Should().Be(created);
        query.CreatedBeforeUtc.Should().Be(updated);
        query.UpdatedAfterUtc.Should().Be(created);
        query.UpdatedBeforeUtc.Should().Be(updated);
    }

    [Fact]
    public void From_WithoutFilters_ShouldUseDefaults()
    {
        var query = CleanseIssueQueryDto.From(
            CleanseIssueSortField.LastUpdatedAtUtc, sortDescending: false, skip: 0, top: 50);

        query.IsActive.Should().BeNull();
        query.IssueCode.Should().BeNull();
        query.CphContains.Should().BeNull();
        query.IsIgnored.Should().BeNull();
        query.IsUnassigned.Should().BeNull();
        query.SortBy.Should().Be(CleanseIssueSortField.LastUpdatedAtUtc);
        query.SortDescending.Should().BeFalse();
    }
}
