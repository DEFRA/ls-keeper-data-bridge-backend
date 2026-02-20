using FluentAssertions;
using KeeperData.Bridge.Controllers;
using KeeperData.Core.Reports.Cleanse.Operations.Queries.Dtos;
using KeeperData.Core.Reports.Issues.Command.AggregateRoots;
using KeeperData.Core.Reports.Issues.Query.Dtos;

namespace KeeperData.Bridge.Tests.Component.Controllers;

public class CleanseControllerResponsesTests
{
    [Fact]
    public void StartAnalysisResponse_RequiredProperties_MustBeSet()
    {
        var response = new StartAnalysisResponse
        {
            OperationId = "op-123",
            Status = "Running",
            Message = "Analysis started"
        };

        response.OperationId.Should().Be("op-123");
        response.Status.Should().Be("Running");
        response.Message.Should().Be("Analysis started");
    }

    [Fact]
    public void StartAnalysisResponse_OptionalProperties_HaveDefaultValues()
    {
        var response = new StartAnalysisResponse
        {
            OperationId = "op-123",
            Status = "Running",
            Message = "OK"
        };

        response.StartedAtUtc.Should().Be(default);
    }

    [Fact]
    public void RegenerateUrlResponse_RequiredProperties_MustBeSet()
    {
        var response = new RegenerateUrlResponse
        {
            OperationId = "op-456",
            ObjectKey = "reports/cleanse/report.csv",
            ReportUrl = "https://s3.amazonaws.com/bucket/report.csv?signature=abc"
        };

        response.OperationId.Should().Be("op-456");
        response.ObjectKey.Should().Contain("report.csv");
        response.ReportUrl.Should().Contain("s3.amazonaws.com");
    }

    [Fact]
    public void DeleteDataResponse_AllProperties_CanBeSet()
    {
        var deletedAt = DateTime.UtcNow;

        var response = new DeleteDataResponse
        {
            Success = true,
            CollectionName = "cleanse_issues",
            DeletedCount = 150,
            Message = "Successfully deleted 150 documents",
            DeletedAtUtc = deletedAt
        };

        response.Success.Should().BeTrue();
        response.CollectionName.Should().Be("cleanse_issues");
        response.DeletedCount.Should().Be(150);
        response.Message.Should().Contain("150");
        response.DeletedAtUtc.Should().Be(deletedAt);
    }

    [Fact]
    public void AnalysisRunsResponse_AllProperties_CanBeSet()
    {
        var runs = new List<CleanseAnalysisOperationSummaryDto>
        {
            new() { Id = "run-1", Status = "Completed" },
            new() { Id = "run-2", Status = "Running" }
        };

        var response = new AnalysisRunsResponse
        {
            Skip = 0,
            Top = 10,
            Count = 2,
            Runs = runs,
            Timestamp = DateTime.UtcNow
        };

        response.Skip.Should().Be(0);
        response.Top.Should().Be(10);
        response.Count.Should().Be(2);
        response.Runs.Should().HaveCount(2);
    }

    [Fact]
    public void IssuesResponse_AllProperties_CanBeSet()
    {
        var issues = new List<IssueDto>
        {
            new() { Id = "issue-1", IssueCode = "CTS_CPH_NOT_IN_SAM", RuleCode = "2A", ErrorCode = "02A", ErrorDescription = "Active CTS CPH inactive / missing in Sam", CtsLidFullIdentifier = "EN-01/001/0001", Cph = "01/001/0001" }
        };

        var response = new IssuesResponse
        {
            Skip = 0,
            Top = 50,
            Count = 1,
            TotalCount = 100,
            Issues = issues,
            Timestamp = DateTime.UtcNow
        };

        response.Skip.Should().Be(0);
        response.Top.Should().Be(50);
        response.Count.Should().Be(1);
        response.TotalCount.Should().Be(100);
        response.Issues.Should().ContainSingle();
    }

    [Fact]
    public void ErrorResponse_RequiredProperties_MustBeSet()
    {
        var response = new ErrorResponse { Message = "Something went wrong" };

        response.Message.Should().Be("Something went wrong");
    }

    [Fact]
    public void ErrorResponse_OptionalProperties_HaveDefaultValues()
    {
        var response = new ErrorResponse { Message = "Error" };

        response.Timestamp.Should().Be(default);
    }

    [Fact]
    public void TestNotificationResponse_AllProperties_CanBeSet()
    {
        var sentAt = DateTime.UtcNow;

        var response = new TestNotificationResponse
        {
            Success = true,
            Recipient = "test@example.com",
            NotificationId = "notify-123",
            Message = "Test notification sent",
            SentAtUtc = sentAt
        };

        response.Success.Should().BeTrue();
        response.Recipient.Should().Be("test@example.com");
        response.NotificationId.Should().Be("notify-123");
        response.Message.Should().Contain("sent");
        response.SentAtUtc.Should().Be(sentAt);
    }

    [Fact]
    public void TestNotificationResponse_NotificationId_CanBeNull()
    {
        var response = new TestNotificationResponse
        {
            Success = false,
            Recipient = "test@example.com",
            NotificationId = null,
            Message = "Failed to send"
        };

        response.NotificationId.Should().BeNull();
    }
}

public class ImportControllerResponsesTests
{
    [Fact]
    public void GenerateRecordIdRequest_RequiredProperties_MustBeSet()
    {
        var request = new GenerateRecordIdRequest
        {
            KeyParts = new[] { "NORTH", "F001" }
        };

        request.KeyParts.Should().HaveCount(2);
        request.KeyParts[0].Should().Be("NORTH");
        request.KeyParts[1].Should().Be("F001");
    }

    [Fact]
    public void GenerateRecordIdResponse_AllProperties_CanBeSet()
    {
        var timestamp = DateTime.UtcNow;

        var response = new GenerateRecordIdResponse
        {
            RecordId = "abc123def456ghi789",
            KeyParts = new[] { "NORTH", "F001" },
            Timestamp = timestamp
        };

        response.RecordId.Should().Be("abc123def456ghi789");
        response.KeyParts.Should().BeEquivalentTo(new[] { "NORTH", "F001" });
        response.Timestamp.Should().Be(timestamp);
    }

    [Fact]
    public void GenerateRecordIdResponse_OptionalProperties_HaveDefaultValues()
    {
        var response = new GenerateRecordIdResponse
        {
            RecordId = "test-id",
            KeyParts = new[] { "key1" }
        };

        response.Timestamp.Should().Be(default);
    }
}
