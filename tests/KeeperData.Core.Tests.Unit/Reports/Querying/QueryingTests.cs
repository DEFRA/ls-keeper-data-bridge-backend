using FluentAssertions;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Dtos;
using KeeperData.Core.Reports.Querying;

namespace KeeperData.Core.Tests.Unit.Reports.Querying;

public class CleanseIssueQueryTests
{
    [Fact]
    public void Create_ReturnsNewInstance()
    {
        var query = CleanseIssueQuery.Create();

        query.Should().NotBeNull();
    }

    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var query = CleanseIssueQuery.Create();

        query.IsActive.Should().BeNull();
        query.IssueCode.Should().BeNull();
        query.CphContains.Should().BeNull();
        query.CphStartsWith.Should().BeNull();
        query.CreatedAfterUtc.Should().BeNull();
        query.CreatedBeforeUtc.Should().BeNull();
        query.UpdatedAfterUtc.Should().BeNull();
        query.UpdatedBeforeUtc.Should().BeNull();
        query.SortBy.Should().Be(CleanseIssueSortField.LastUpdatedAtUtc);
        query.SortDescending.Should().BeTrue();
        query.Skip.Should().Be(0);
        query.Top.Should().Be(50);
    }

    [Fact]
    public void WhereActive_SetsIsActiveToTrue()
    {
        var query = CleanseIssueQuery.Create().WhereActive();

        query.IsActive.Should().BeTrue();
    }

    [Fact]
    public void WhereInactive_SetsIsActiveToFalse()
    {
        var query = CleanseIssueQuery.Create().WhereInactive();

        query.IsActive.Should().BeFalse();
    }

    [Fact]
    public void WithIssueCode_SetsIssueCode()
    {
        var query = CleanseIssueQuery.Create().WithIssueCode("CTS_CPH_NOT_IN_SAM");

        query.IssueCode.Should().Be("CTS_CPH_NOT_IN_SAM");
    }

    [Fact]
    public void WithCphContaining_SetsCphContains()
    {
        var query = CleanseIssueQuery.Create().WithCphContaining("345");

        query.CphContains.Should().Be("345");
    }

    [Fact]
    public void WithCphStartingWith_SetsCphStartsWith()
    {
        var query = CleanseIssueQuery.Create().WithCphStartingWith("12/");

        query.CphStartsWith.Should().Be("12/");
    }

    [Fact]
    public void CreatedAfter_SetsCreatedAfterUtc()
    {
        var dateTime = DateTime.UtcNow.AddDays(-7);
        var query = CleanseIssueQuery.Create().CreatedAfter(dateTime);

        query.CreatedAfterUtc.Should().Be(dateTime);
    }

    [Fact]
    public void CreatedBefore_SetsCreatedBeforeUtc()
    {
        var dateTime = DateTime.UtcNow;
        var query = CleanseIssueQuery.Create().CreatedBefore(dateTime);

        query.CreatedBeforeUtc.Should().Be(dateTime);
    }

    [Fact]
    public void UpdatedAfter_SetsUpdatedAfterUtc()
    {
        var dateTime = DateTime.UtcNow.AddHours(-24);
        var query = CleanseIssueQuery.Create().UpdatedAfter(dateTime);

        query.UpdatedAfterUtc.Should().Be(dateTime);
    }

    [Fact]
    public void UpdatedBefore_SetsUpdatedBeforeUtc()
    {
        var dateTime = DateTime.UtcNow;
        var query = CleanseIssueQuery.Create().UpdatedBefore(dateTime);

        query.UpdatedBeforeUtc.Should().Be(dateTime);
    }

    [Fact]
    public void OrderBy_SetsSortByAndDirection()
    {
        var query = CleanseIssueQuery.Create().OrderBy(CleanseIssueSortField.Cph, descending: true);

        query.SortBy.Should().Be(CleanseIssueSortField.Cph);
        query.SortDescending.Should().BeTrue();
    }

    [Fact]
    public void OrderBy_DefaultsToAscending()
    {
        var query = CleanseIssueQuery.Create().OrderBy(CleanseIssueSortField.CreatedAtUtc);

        query.SortBy.Should().Be(CleanseIssueSortField.CreatedAtUtc);
        query.SortDescending.Should().BeFalse();
    }

    [Fact]
    public void Page_SetsSkipAndTop()
    {
        var query = CleanseIssueQuery.Create().Page(skip: 20, top: 10);

        query.Skip.Should().Be(20);
        query.Top.Should().Be(10);
    }

    [Fact]
    public void FluentChaining_WorksCorrectly()
    {
        var after = DateTime.UtcNow.AddDays(-30);

        var query = CleanseIssueQuery.Create()
            .WhereActive()
            .WithIssueCode("CTS_CPH_NOT_IN_SAM")
            .WithCphStartingWith("12/")
            .CreatedAfter(after)
            .OrderBy(CleanseIssueSortField.Cph, descending: false)
            .Page(skip: 0, top: 100);

        query.IsActive.Should().BeTrue();
        query.IssueCode.Should().Be("CTS_CPH_NOT_IN_SAM");
        query.CphStartsWith.Should().Be("12/");
        query.CreatedAfterUtc.Should().Be(after);
        query.SortBy.Should().Be(CleanseIssueSortField.Cph);
        query.SortDescending.Should().BeFalse();
        query.Skip.Should().Be(0);
        query.Top.Should().Be(100);
    }

    [Fact]
    public void MethodChaining_ReturnsSameInstance()
    {
        var query = CleanseIssueQuery.Create();

        var result = query.WhereActive();

        result.Should().BeSameAs(query);
    }
}

public class CleanseIssueQueryResultTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var items = new List<CleanseReportItem>();

        var result = new CleanseIssueQueryResult
        {
            Items = items,
            TotalCount = 0,
            Skip = 0,
            Top = 50
        };

        result.Items.Should().BeEmpty();
        result.TotalCount.Should().Be(0);
        result.Skip.Should().Be(0);
        result.Top.Should().Be(50);
    }

    [Fact]
    public void HasMore_ReturnsTrueWhenMoreItemsAvailable()
    {
        var items = new List<CleanseReportItem>
        {
            new() { Id = "1", Code = "TEST", CtsLidFullIdentifier = "EN-01/001/0001", Cph = "01/001/0001" },
            new() { Id = "2", Code = "TEST", CtsLidFullIdentifier = "EN-01/001/0002", Cph = "01/001/0002" }
        };

        var result = new CleanseIssueQueryResult
        {
            Items = items,
            TotalCount = 100,
            Skip = 0,
            Top = 2
        };

        result.HasMore.Should().BeTrue();
    }

    [Fact]
    public void HasMore_ReturnsFalseWhenNoMoreItems()
    {
        var items = new List<CleanseReportItem>
        {
            new() { Id = "1", Code = "TEST", CtsLidFullIdentifier = "EN-01/001/0001", Cph = "01/001/0001" }
        };

        var result = new CleanseIssueQueryResult
        {
            Items = items,
            TotalCount = 1,
            Skip = 0,
            Top = 50
        };

        result.HasMore.Should().BeFalse();
    }

    [Fact]
    public void HasMore_ReturnsFalseWhenAtLastPage()
    {
        var items = new List<CleanseReportItem>
        {
            new() { Id = "1", Code = "TEST", CtsLidFullIdentifier = "EN-01/001/0001", Cph = "01/001/0001" }
        };

        var result = new CleanseIssueQueryResult
        {
            Items = items,
            TotalCount = 51,
            Skip = 50,
            Top = 50
        };

        result.HasMore.Should().BeFalse();
    }

    [Fact]
    public void Record_IsSealed()
    {
        typeof(CleanseIssueQueryResult).IsSealed.Should().BeTrue();
    }
}

public class CleanseIssueGroupResultTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var items = new List<CleanseReportItem>();

        var result = new CleanseIssueGroupResult
        {
            IssueCode = "CTS_CPH_NOT_IN_SAM",
            TotalCount = 0,
            Items = items
        };

        result.IssueCode.Should().Be("CTS_CPH_NOT_IN_SAM");
        result.TotalCount.Should().Be(0);
        result.Items.Should().BeEmpty();
    }

    [Fact]
    public void CanRepresentGroupWithSample()
    {
        var items = new List<CleanseReportItem>
        {
            new() { Id = "1", Code = "CTS_CPH_NOT_IN_SAM", CtsLidFullIdentifier = "EN-01/001/0001", Cph = "01/001/0001" },
            new() { Id = "2", Code = "CTS_CPH_NOT_IN_SAM", CtsLidFullIdentifier = "EN-01/001/0002", Cph = "01/001/0002" }
        };

        var result = new CleanseIssueGroupResult
        {
            IssueCode = "CTS_CPH_NOT_IN_SAM",
            TotalCount = 500,
            Items = items
        };

        result.TotalCount.Should().Be(500);
        result.Items.Should().HaveCount(2);
    }

    [Fact]
    public void Record_IsSealed()
    {
        typeof(CleanseIssueGroupResult).IsSealed.Should().BeTrue();
    }
}

public class CleanseIssueSortFieldTests
{
    [Fact]
    public void Enum_HasFourValues()
    {
        var values = Enum.GetValues<CleanseIssueSortField>();

        values.Should().HaveCount(4);
    }

    [Theory]
    [InlineData(CleanseIssueSortField.Cph)]
    [InlineData(CleanseIssueSortField.IssueCode)]
    [InlineData(CleanseIssueSortField.CreatedAtUtc)]
    [InlineData(CleanseIssueSortField.LastUpdatedAtUtc)]
    public void AllValues_AreDefined(CleanseIssueSortField field)
    {
        Enum.IsDefined(typeof(CleanseIssueSortField), field).Should().BeTrue();
    }
}

public class CleanseIssuesResultTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var items = new List<CleanseReportItem>();

        var result = new CleanseIssuesResult { Items = items };

        result.Items.Should().BeEmpty();
    }

    [Fact]
    public void OptionalProperties_HaveDefaultValues()
    {
        var result = new CleanseIssuesResult { Items = [] };

        result.TotalCount.Should().Be(0);
        result.Skip.Should().Be(0);
        result.Top.Should().Be(0);
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var items = new List<CleanseReportItem>
        {
            new() { Id = "1", Code = "TEST", CtsLidFullIdentifier = "EN-01/001/0001", Cph = "01/001/0001" }
        };

        var result = new CleanseIssuesResult
        {
            Items = items,
            TotalCount = 100,
            Skip = 20,
            Top = 10
        };

        result.Items.Should().ContainSingle();
        result.TotalCount.Should().Be(100);
        result.Skip.Should().Be(20);
        result.Top.Should().Be(10);
    }
}

public class RegenerateReportUrlResultTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var result = new RegenerateReportUrlResult { Success = true };

        result.Success.Should().BeTrue();
    }

    [Fact]
    public void OptionalProperties_HaveDefaultValues()
    {
        var result = new RegenerateReportUrlResult { Success = false };

        result.OperationId.Should().BeNull();
        result.ReportUrl.Should().BeNull();
        result.ObjectKey.Should().BeNull();
        result.Error.Should().BeNull();
    }

    [Fact]
    public void SuccessfulResult_HasUrlAndKey()
    {
        var result = new RegenerateReportUrlResult
        {
            Success = true,
            OperationId = "op-123",
            ReportUrl = "https://s3.amazonaws.com/bucket/report.csv?signature=abc",
            ObjectKey = "reports/cleanse/2024-06-15/report.csv"
        };

        result.Success.Should().BeTrue();
        result.ReportUrl.Should().Contain("s3.amazonaws.com");
        result.ObjectKey.Should().Contain("report.csv");
        result.Error.Should().BeNull();
    }

    [Fact]
    public void FailedResult_HasError()
    {
        var result = new RegenerateReportUrlResult
        {
            Success = false,
            OperationId = "op-456",
            Error = "Report not found"
        };

        result.Success.Should().BeFalse();
        result.ReportUrl.Should().BeNull();
        result.Error.Should().Be("Report not found");
    }
}
