using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.Database;
using KeeperData.Core.Reports.Abstract;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Impl;
using KeeperData.Infrastructure.Database.Configuration;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Cleanse;

/// <summary>
/// Integration tests for CleanseReportRepository and CleanseAnalysisRepository.
/// </summary>
[Collection("MongoDB"), Trait("Dependence", "docker")]
public class CleanseRepositoryTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly MongoDbFixture _mongoDbFixture;
    private readonly IMongoClient _mongoClient;
    private readonly string _testDatabaseName = "cleanse-repo-test-db";

    private ICleanseReportRepository _reportRepository = null!;
    private ICleanseAnalysisRepository _analysisRepository = null!;

    public CleanseRepositoryTests(
        ITestOutputHelper testOutputHelper,
        MongoDbFixture mongoDbFixture)
    {
        _testOutputHelper = testOutputHelper;
        _mongoDbFixture = mongoDbFixture;
        _mongoClient = _mongoDbFixture.MongoClient;
    }

    public async Task InitializeAsync()
    {
        await _mongoClient.DropDatabaseAsync(_testDatabaseName);

        var mongoConfig = Options.Create<IDatabaseConfig>(new MongoConfig
        {
            DatabaseName = _testDatabaseName,
            DatabaseUri = _mongoDbFixture.ConnectionString
        });

        _reportRepository = new CleanseReportRepository(_mongoClient, mongoConfig);
        _analysisRepository = new CleanseAnalysisRepository(_mongoClient, mongoConfig);

        _testOutputHelper.WriteLine($"Initialized test database: {_testDatabaseName}");
    }

    public async Task DisposeAsync()
    {
        await _mongoClient.DropDatabaseAsync(_testDatabaseName);
    }

    #region CleanseReportRepository Tests

    [Fact]
    public async Task ReportRepo_Upsert_ShouldInsertNewItem()
    {
        // Arrange
        var item = CreateTestReportItem("test-id-1");

        // Act
        await _reportRepository.UpsertAsync(item);

        // Assert
        var retrieved = await _reportRepository.GetByIdAsync("test-id-1");
        retrieved.Should().NotBeNull();
        retrieved!.Code.Should().Be(item.Code);
        retrieved.CtsLidFullIdentifier.Should().Be(item.CtsLidFullIdentifier);
        retrieved.Cph.Should().Be(item.Cph);
        retrieved.IsActive.Should().BeTrue();

        _testOutputHelper.WriteLine("Item inserted successfully");
    }

    [Fact]
    public async Task ReportRepo_Upsert_ShouldUpdateExistingItem()
    {
        // Arrange
        var item = CreateTestReportItem("test-id-2");
        await _reportRepository.UpsertAsync(item);

        // Act - Update the item
        item.Cph = "99/999/9999";
        item.LastUpdatedAtUtc = DateTime.UtcNow;
        await _reportRepository.UpsertAsync(item);

        // Assert
        var retrieved = await _reportRepository.GetByIdAsync("test-id-2");
        retrieved.Should().NotBeNull();
        retrieved!.Cph.Should().Be("99/999/9999");

        _testOutputHelper.WriteLine("Item updated successfully");
    }

    [Fact]
    public async Task ReportRepo_GetById_WithNonExistentId_ShouldReturnNull()
    {
        // Act
        var result = await _reportRepository.GetByIdAsync("non-existent-id");

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public async Task ReportRepo_Activate_ShouldSetIsActiveTrue()
    {
        // Arrange
        var item = CreateTestReportItem("test-id-3");
        item.IsActive = false;
        await _reportRepository.UpsertAsync(item);

        // Act
        var timestamp = DateTime.UtcNow;
        await _reportRepository.ActivateAsync("test-id-3", timestamp);

        // Assert
        var retrieved = await _reportRepository.GetByIdAsync("test-id-3");
        retrieved.Should().NotBeNull();
        retrieved!.IsActive.Should().BeTrue();
        retrieved.LastUpdatedAtUtc.Should().BeCloseTo(timestamp, TimeSpan.FromSeconds(1));

        _testOutputHelper.WriteLine("Item activated successfully");
    }

    [Fact]
    public async Task ReportRepo_Deactivate_ShouldSetIsActiveFalse()
    {
        // Arrange
        var item = CreateTestReportItem("test-id-4");
        item.IsActive = true;
        await _reportRepository.UpsertAsync(item);

        // Act
        var timestamp = DateTime.UtcNow;
        await _reportRepository.DeactivateAsync("test-id-4", timestamp);

        // Assert
        var retrieved = await _reportRepository.GetByIdAsync("test-id-4");
        retrieved.Should().NotBeNull();
        retrieved!.IsActive.Should().BeFalse();
        retrieved.LastUpdatedAtUtc.Should().BeCloseTo(timestamp, TimeSpan.FromSeconds(1));

        _testOutputHelper.WriteLine("Item deactivated successfully");
    }

    [Fact]
    public async Task ReportRepo_GetActiveIssues_ShouldReturnOnlyActiveItems()
    {
        // Arrange
        var activeItem1 = CreateTestReportItem("active-1");
        activeItem1.IsActive = true;
        await _reportRepository.UpsertAsync(activeItem1);

        var activeItem2 = CreateTestReportItem("active-2");
        activeItem2.IsActive = true;
        await _reportRepository.UpsertAsync(activeItem2);

        var inactiveItem = CreateTestReportItem("inactive-1");
        inactiveItem.IsActive = false;
        await _reportRepository.UpsertAsync(inactiveItem);

        // Act
        var activeIssues = await _reportRepository.GetActiveIssuesAsync(0, 10);

        // Assert
        activeIssues.Should().HaveCount(2);
        activeIssues.Should().AllSatisfy(i => i.IsActive.Should().BeTrue());

        _testOutputHelper.WriteLine($"Retrieved {activeIssues.Count} active issues");
    }

    [Fact]
    public async Task ReportRepo_GetActiveIssues_ShouldRespectPagination()
    {
        // Arrange
        for (var i = 1; i <= 5; i++)
        {
            var item = CreateTestReportItem($"paginated-{i}");
            item.IsActive = true;
            await _reportRepository.UpsertAsync(item);
        }

        // Act
        var firstPage = await _reportRepository.GetActiveIssuesAsync(0, 2);
        var secondPage = await _reportRepository.GetActiveIssuesAsync(2, 2);

        // Assert
        firstPage.Should().HaveCount(2);
        secondPage.Should().HaveCount(2);

        _testOutputHelper.WriteLine("Pagination working correctly");
    }

    #endregion

    #region CleanseAnalysisRepository Tests

    [Fact]
    public async Task AnalysisRepo_CreateOperation_ShouldInsertOperation()
    {
        // Arrange
        var operation = CreateTestOperation("op-1");

        // Act
        await _analysisRepository.CreateOperationAsync(operation);

        // Assert
        var retrieved = await _analysisRepository.GetOperationAsync("op-1");
        retrieved.Should().NotBeNull();
        retrieved!.Status.Should().Be(CleanseAnalysisStatus.Running);
        retrieved.StatusDescription.Should().Be("Test operation");

        _testOutputHelper.WriteLine("Operation created successfully");
    }

    [Fact]
    public async Task AnalysisRepo_UpdateProgress_ShouldUpdateFields()
    {
        // Arrange
        var operation = CreateTestOperation("op-2");
        await _analysisRepository.CreateOperationAsync(operation);

        // Act
        await _analysisRepository.UpdateProgressAsync(
            "op-2",
            progressPercentage: 50.0,
            statusDescription: "Halfway done",
            recordsAnalyzed: 100,
            issuesFound: 5,
            issuesResolved: 2);

        // Assert
        var retrieved = await _analysisRepository.GetOperationAsync("op-2");
        retrieved.Should().NotBeNull();
        retrieved!.ProgressPercentage.Should().Be(50.0);
        retrieved.StatusDescription.Should().Be("Halfway done");
        retrieved.RecordsAnalyzed.Should().Be(100);
        retrieved.IssuesFound.Should().Be(5);
        retrieved.IssuesResolved.Should().Be(2);

        _testOutputHelper.WriteLine("Progress updated successfully");
    }

    [Fact]
    public async Task AnalysisRepo_CompleteOperation_ShouldSetCompletedStatus()
    {
        // Arrange
        var operation = CreateTestOperation("op-3");
        await _analysisRepository.CreateOperationAsync(operation);

        // Act
        await _analysisRepository.CompleteOperationAsync(
            "op-3",
            recordsAnalyzed: 200,
            issuesFound: 10,
            issuesResolved: 5,
            durationMs: 5000);

        // Assert
        var retrieved = await _analysisRepository.GetOperationAsync("op-3");
        retrieved.Should().NotBeNull();
        retrieved!.Status.Should().Be(CleanseAnalysisStatus.Completed);
        retrieved.ProgressPercentage.Should().Be(100.0);
        retrieved.RecordsAnalyzed.Should().Be(200);
        retrieved.IssuesFound.Should().Be(10);
        retrieved.IssuesResolved.Should().Be(5);
        retrieved.DurationMs.Should().Be(5000);
        retrieved.CompletedAtUtc.Should().NotBeNull();

        _testOutputHelper.WriteLine("Operation completed successfully");
    }

    [Fact]
    public async Task AnalysisRepo_FailOperation_ShouldSetFailedStatus()
    {
        // Arrange
        var operation = CreateTestOperation("op-4");
        await _analysisRepository.CreateOperationAsync(operation);

        // Act
        await _analysisRepository.FailOperationAsync(
            "op-4",
            error: "Test error message",
            durationMs: 1000);

        // Assert
        var retrieved = await _analysisRepository.GetOperationAsync("op-4");
        retrieved.Should().NotBeNull();
        retrieved!.Status.Should().Be(CleanseAnalysisStatus.Failed);
        retrieved.Error.Should().Be("Test error message");
        retrieved.DurationMs.Should().Be(1000);
        retrieved.CompletedAtUtc.Should().NotBeNull();

        _testOutputHelper.WriteLine("Operation failed as expected");
    }

    [Fact]
    public async Task AnalysisRepo_GetOperations_ShouldReturnOrderedByStartTime()
    {
        // Arrange
        for (var i = 1; i <= 3; i++)
        {
            var operation = CreateTestOperation($"ordered-op-{i}");
            operation.StartedAtUtc = DateTime.UtcNow.AddMinutes(-i);
            await _analysisRepository.CreateOperationAsync(operation);
            await Task.Delay(10); // Ensure different timestamps
        }

        // Act
        var operations = await _analysisRepository.GetOperationsAsync(0, 10);

        // Assert
        operations.Should().HaveCount(3);
        operations.Should().BeInDescendingOrder(o => o.StartedAtUtc);

        _testOutputHelper.WriteLine("Operations returned in correct order");
    }

    [Fact]
    public async Task AnalysisRepo_GetCurrentOperation_ShouldReturnRunningOperation()
    {
        // Arrange
        var completedOp = CreateTestOperation("completed-op");
        completedOp.Status = CleanseAnalysisStatus.Completed;
        await _analysisRepository.CreateOperationAsync(completedOp);

        var runningOp = CreateTestOperation("running-op");
        runningOp.Status = CleanseAnalysisStatus.Running;
        await _analysisRepository.CreateOperationAsync(runningOp);

        // Act
        var current = await _analysisRepository.GetCurrentOperationAsync();

        // Assert
        current.Should().NotBeNull();
        current!.Id.Should().Be("running-op");
        current.Status.Should().Be(CleanseAnalysisStatus.Running);

        _testOutputHelper.WriteLine("Current running operation retrieved correctly");
    }

    [Fact]
    public async Task AnalysisRepo_GetCurrentOperation_WhenNoneRunning_ShouldReturnNull()
    {
        // Arrange
        var completedOp = CreateTestOperation("only-completed-op");
        completedOp.Status = CleanseAnalysisStatus.Completed;
        await _analysisRepository.CreateOperationAsync(completedOp);

        // Act
        var current = await _analysisRepository.GetCurrentOperationAsync();

        // Assert
        current.Should().BeNull();

        _testOutputHelper.WriteLine("No current operation when none running");
    }

    [Fact]
    public async Task AnalysisRepo_GetOperation_WithNonExistentId_ShouldReturnNull()
    {
        // Act
        var result = await _analysisRepository.GetOperationAsync("non-existent-op");

        // Assert
        result.Should().BeNull();
    }

    #endregion

    #region Helper Methods

    private static CleanseReportItem CreateTestReportItem(string id) => new()
    {
        Id = id,
        Code = "CTS_CPH_NOT_IN_SAM",
        CtsLidFullIdentifier = $"AH-{id}/345/6789",
        Cph = "12/345/6789",
        CreatedAtUtc = DateTime.UtcNow,
        LastUpdatedAtUtc = DateTime.UtcNow,
        IsActive = true
    };

    private static CleanseAnalysisOperation CreateTestOperation(string id) => new()
    {
        Id = id,
        Status = CleanseAnalysisStatus.Running,
        StartedAtUtc = DateTime.UtcNow,
        StatusDescription = "Test operation",
        ProgressPercentage = 0,
        RecordsAnalyzed = 0,
        TotalRecords = 0,
        IssuesFound = 0,
        IssuesResolved = 0
    };

    #endregion
}
