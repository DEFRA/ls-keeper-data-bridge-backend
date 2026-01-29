using FluentAssertions;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Dtos;

namespace KeeperData.Core.Tests.Unit.Reports.Dtos;

public class CleanseDeleteResultTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var result = new CleanseDeleteResult
        {
            Success = true,
            CollectionName = "cleanse_issues",
            Message = "Collection deleted successfully"
        };

        result.Success.Should().BeTrue();
        result.CollectionName.Should().Be("cleanse_issues");
        result.Message.Should().Be("Collection deleted successfully");
    }

    [Fact]
    public void OptionalProperties_HaveDefaultValues()
    {
        var result = new CleanseDeleteResult
        {
            Success = true,
            CollectionName = "test",
            Message = "OK"
        };

        result.DeletedCount.Should().BeNull();
        result.OperatedAtUtc.Should().Be(default);
        result.Error.Should().BeNull();
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var operatedAt = DateTime.UtcNow;
        var error = new InvalidOperationException("Test error");

        var result = new CleanseDeleteResult
        {
            Success = false,
            CollectionName = "cleanse_issues",
            Message = "Failed to delete collection",
            DeletedCount = 0,
            OperatedAtUtc = operatedAt,
            Error = error
        };

        result.Success.Should().BeFalse();
        result.CollectionName.Should().Be("cleanse_issues");
        result.Message.Should().Be("Failed to delete collection");
        result.DeletedCount.Should().Be(0);
        result.OperatedAtUtc.Should().Be(operatedAt);
        result.Error.Should().Be(error);
    }

    [Fact]
    public void SuccessfulDeletion_WithCount()
    {
        var result = new CleanseDeleteResult
        {
            Success = true,
            CollectionName = "cleanse_issues",
            Message = "Deleted 150 documents",
            DeletedCount = 150,
            OperatedAtUtc = DateTime.UtcNow
        };

        result.Success.Should().BeTrue();
        result.DeletedCount.Should().Be(150);
    }
}

public class CleanseAnalysisOperationSummaryTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var summary = new CleanseAnalysisOperationSummary
        {
            Id = "op-123",
            Status = "Running"
        };

        summary.Id.Should().Be("op-123");
        summary.Status.Should().Be("Running");
    }

    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var summary = new CleanseAnalysisOperationSummary
        {
            Id = "op-123",
            Status = "Pending"
        };

        summary.StartedAtUtc.Should().Be(default);
        summary.CompletedAtUtc.Should().BeNull();
        summary.ProgressPercentage.Should().Be(0);
        summary.RecordsAnalyzed.Should().Be(0);
        summary.IssuesFound.Should().Be(0);
        summary.IssuesResolved.Should().Be(0);
        summary.DurationMs.Should().BeNull();
        summary.ReportObjectKey.Should().BeNull();
        summary.ReportUrl.Should().BeNull();
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var startedAt = DateTime.UtcNow.AddMinutes(-30);
        var completedAt = DateTime.UtcNow;

        var summary = new CleanseAnalysisOperationSummary
        {
            Id = "op-456",
            Status = "Completed",
            StartedAtUtc = startedAt,
            CompletedAtUtc = completedAt,
            ProgressPercentage = 100.0,
            RecordsAnalyzed = 50000,
            IssuesFound = 250,
            IssuesResolved = 100,
            DurationMs = 1800000,
            ReportObjectKey = "reports/cleanse/2024-06-15/report.csv",
            ReportUrl = "https://s3.amazonaws.com/bucket/reports/cleanse/2024-06-15/report.csv?signature=abc"
        };

        summary.Id.Should().Be("op-456");
        summary.Status.Should().Be("Completed");
        summary.StartedAtUtc.Should().Be(startedAt);
        summary.CompletedAtUtc.Should().Be(completedAt);
        summary.ProgressPercentage.Should().Be(100.0);
        summary.RecordsAnalyzed.Should().Be(50000);
        summary.IssuesFound.Should().Be(250);
        summary.IssuesResolved.Should().Be(100);
        summary.DurationMs.Should().Be(1800000);
        summary.ReportObjectKey.Should().Contain("report.csv");
        summary.ReportUrl.Should().Contain("s3.amazonaws.com");
    }

    [Theory]
    [InlineData("Pending")]
    [InlineData("Running")]
    [InlineData("Completed")]
    [InlineData("Failed")]
    public void Status_CanBeAnyString(string status)
    {
        var summary = new CleanseAnalysisOperationSummary
        {
            Id = "op-123",
            Status = status
        };

        summary.Status.Should().Be(status);
    }

    [Fact]
    public void ProgressPercentage_CanBePartial()
    {
        var summary = new CleanseAnalysisOperationSummary
        {
            Id = "op-123",
            Status = "Running",
            ProgressPercentage = 45.5
        };

        summary.ProgressPercentage.Should().Be(45.5);
    }
}

public class CleanseReportItemTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var item = new CleanseReportItem
        {
            Id = "thumb-123",
            Code = "CTS_CPH_NOT_IN_SAM",
            CtsLidFullIdentifier = "EN-12/345/6789",
            Cph = "12/345/6789"
        };

        item.Id.Should().Be("thumb-123");
        item.Code.Should().Be("CTS_CPH_NOT_IN_SAM");
        item.CtsLidFullIdentifier.Should().Be("EN-12/345/6789");
        item.Cph.Should().Be("12/345/6789");
    }

    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var item = new CleanseReportItem
        {
            Id = "test",
            Code = "TEST",
            CtsLidFullIdentifier = "EN-01/001/0001",
            Cph = "01/001/0001"
        };

        item.CreatedAtUtc.Should().Be(default);
        item.LastUpdatedAtUtc.Should().Be(default);
        item.IsActive.Should().BeFalse();
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var createdAt = DateTime.UtcNow.AddDays(-30);
        var updatedAt = DateTime.UtcNow;

        var item = new CleanseReportItem
        {
            Id = "thumb-456",
            Code = "CTS_CPH_NOT_IN_SAM",
            CtsLidFullIdentifier = "WA-99/888/7777",
            Cph = "99/888/7777",
            CreatedAtUtc = createdAt,
            LastUpdatedAtUtc = updatedAt,
            IsActive = true
        };

        item.CreatedAtUtc.Should().Be(createdAt);
        item.LastUpdatedAtUtc.Should().Be(updatedAt);
        item.IsActive.Should().BeTrue();
    }

    [Fact]
    public void Properties_CanBeModified()
    {
        var item = new CleanseReportItem
        {
            Id = "test",
            Code = "TEST",
            CtsLidFullIdentifier = "EN-01/001/0001",
            Cph = "01/001/0001",
            IsActive = true
        };

        item.IsActive = false;
        item.LastUpdatedAtUtc = DateTime.UtcNow;

        item.IsActive.Should().BeFalse();
        item.LastUpdatedAtUtc.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));
    }
}

public class CleanseAnalysisOperationTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var operation = new CleanseAnalysisOperation { Id = "op-123" };

        operation.Id.Should().Be("op-123");
    }

    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var operation = new CleanseAnalysisOperation { Id = "test" };

        operation.Status.Should().Be(CleanseAnalysisStatus.NotStarted);
        operation.StartedAtUtc.Should().Be(default);
        operation.CompletedAtUtc.Should().BeNull();
        operation.ProgressPercentage.Should().Be(0);
        operation.StatusDescription.Should().BeEmpty();
        operation.RecordsAnalyzed.Should().Be(0);
        operation.TotalRecords.Should().Be(0);
        operation.IssuesFound.Should().Be(0);
        operation.IssuesResolved.Should().Be(0);
        operation.Error.Should().BeNull();
        operation.DurationMs.Should().BeNull();
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var startedAt = DateTime.UtcNow.AddMinutes(-30);
        var completedAt = DateTime.UtcNow;

        var operation = new CleanseAnalysisOperation
        {
            Id = "op-456",
            Status = CleanseAnalysisStatus.Completed,
            StartedAtUtc = startedAt,
            CompletedAtUtc = completedAt,
            ProgressPercentage = 100.0,
            StatusDescription = "Analysis completed successfully",
            RecordsAnalyzed = 50000,
            TotalRecords = 50000,
            IssuesFound = 250,
            IssuesResolved = 100,
            Error = null,
            DurationMs = 1800000
        };

        operation.Status.Should().Be(CleanseAnalysisStatus.Completed);
        operation.ProgressPercentage.Should().Be(100.0);
        operation.RecordsAnalyzed.Should().Be(50000);
        operation.TotalRecords.Should().Be(50000);
        operation.IssuesFound.Should().Be(250);
        operation.IssuesResolved.Should().Be(100);
        operation.DurationMs.Should().Be(1800000);
    }

    [Fact]
    public void CanRepresentFailedOperation()
    {
        var operation = new CleanseAnalysisOperation
        {
            Id = "op-failed",
            Status = CleanseAnalysisStatus.Failed,
            Error = "Database connection failed",
            ProgressPercentage = 25.5
        };

        operation.Status.Should().Be(CleanseAnalysisStatus.Failed);
        operation.Error.Should().Contain("Database");
        operation.ProgressPercentage.Should().Be(25.5);
    }

    [Theory]
    [InlineData(CleanseAnalysisStatus.NotStarted)]
    [InlineData(CleanseAnalysisStatus.Running)]
    [InlineData(CleanseAnalysisStatus.Completed)]
    [InlineData(CleanseAnalysisStatus.Failed)]
    [InlineData(CleanseAnalysisStatus.Cancelled)]
    public void Status_CanBeAnyValidValue(CleanseAnalysisStatus status)
    {
        var operation = new CleanseAnalysisOperation
        {
            Id = "test",
            Status = status
        };

        operation.Status.Should().Be(status);
    }

    [Fact]
    public void Properties_CanBeModified()
    {
        var operation = new CleanseAnalysisOperation { Id = "op-123" };

        operation.Status = CleanseAnalysisStatus.Running;
        operation.RecordsAnalyzed = 1000;
        operation.ProgressPercentage = 50.0;

        operation.Status.Should().Be(CleanseAnalysisStatus.Running);
        operation.RecordsAnalyzed.Should().Be(1000);
        operation.ProgressPercentage.Should().Be(50.0);
    }
}

public class CleanseAnalysisStatusTests
{
    [Fact]
    public void Enum_HasFiveValues()
    {
        var values = Enum.GetValues<CleanseAnalysisStatus>();

        values.Should().HaveCount(5);
    }

    [Theory]
    [InlineData(CleanseAnalysisStatus.NotStarted, 0)]
    [InlineData(CleanseAnalysisStatus.Running, 1)]
    [InlineData(CleanseAnalysisStatus.Completed, 2)]
    [InlineData(CleanseAnalysisStatus.Failed, 3)]
    [InlineData(CleanseAnalysisStatus.Cancelled, 4)]
    public void EnumValues_HaveExpectedNumericValues(CleanseAnalysisStatus status, int expectedValue)
    {
        ((int)status).Should().Be(expectedValue);
    }
}
