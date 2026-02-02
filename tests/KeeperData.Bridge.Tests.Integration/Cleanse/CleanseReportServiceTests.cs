using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.Database;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Locking;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Impl;
using KeeperData.Core.Reports;
using KeeperData.Core.Reports.Abstract;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Impl;
using KeeperData.Core.Reports.Strategies;
using KeeperData.Core.Storage;
using KeeperData.Infrastructure.Database.Configuration;
using KeeperData.Infrastructure.Locking;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Cleanse;

/// <summary>
/// Integration tests for CleanseReportService using TestContainers MongoDB.
/// Tests cleanse analysis functionality including issue detection, resolution, and progress tracking.
/// </summary>
[Collection("MongoDB"), Trait("Dependence", "docker")]
public class CleanseReportServiceTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly MongoDbFixture _mongoDbFixture;
    private readonly IMongoClient _mongoClient;
    private readonly string _testDatabaseName = "cleanse-test-db";

    private CleanseReportService _sut = null!;
    private ICleanseReportRepository _reportRepository = null!;
    private ICleanseAnalysisRepository _analysisRepository = null!;
    private IQueryService _queryService = null!;
    private IDistributedLock _distributedLock = null!;
    private DataSetDefinitions _dataSets = null!;

    // Collection names from StandardDataSetDefinitionsBuilder
    private const string CtsCphHoldingCollection = "cts_cph_holding";
    private const string SamCphHoldingCollection = "sam_cph_holdings";
    private const string CleanseReportCollection = "cleanse_report";
    private const string CleanseAnalysisCollection = "cleanse_analysis_operations";

    public CleanseReportServiceTests(
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
        _testOutputHelper.WriteLine($"Initialized test database: {_testDatabaseName}");

        InitializeServices();
    }

    public async Task DisposeAsync()
    {
        await _mongoClient.DropDatabaseAsync(_testDatabaseName);
        _testOutputHelper.WriteLine($"Cleaned up test database: {_testDatabaseName}");
    }

    private void InitializeServices()
    {
        var mongoConfig = Options.Create<IDatabaseConfig>(new MongoConfig
        {
            DatabaseName = _testDatabaseName,
            DatabaseUri = _mongoDbFixture.ConnectionString
        });

        _dataSets = StandardDataSetDefinitionsBuilder.Build();

        var queryServiceLogger = new Mock<ILogger<QueryService>>();
        _queryService = new QueryService(
            _mongoClient,
            mongoConfig,
            _dataSets,
            queryServiceLogger.Object);

        _reportRepository = new CleanseReportRepository(_mongoClient, mongoConfig);
        _analysisRepository = new CleanseAnalysisRepository(_mongoClient, mongoConfig);
        _distributedLock = new MongoDistributedLock(
            Options.Create(new MongoConfig { DatabaseName = _testDatabaseName }),
            _mongoClient);


        var strategies = new ICleanseAnalysisStrategy[] { new CtsSamAnalysisStrategy() };

        var exportServiceMock = new Mock<ICleanseReportExportService>();
        exportServiceMock
            .Setup(x => x.ExportAndUploadAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new CleanseReportExportResult { Success = true, ReportUrl = "https://example.com/report.zip", ObjectKey = "cleanse-reports/test.zip" });

        var blobStorageServiceMock = new Mock<IBlobStorageService>();
        blobStorageServiceMock
            .Setup(x => x.GeneratePresignedUrl(It.IsAny<string>(), It.IsAny<TimeSpan?>()))
            .Returns("https://example.com/regenerated-report.zip");

        var blobStorageServiceFactoryMock = new Mock<IBlobStorageServiceFactory>();
        blobStorageServiceFactoryMock
            .Setup(x => x.GetCleanseReportsBlobService())
            .Returns(blobStorageServiceMock.Object);

        var notificationServiceMock = new Mock<ICleanseReportNotificationService>();
        notificationServiceMock
            .Setup(x => x.SendReportNotificationAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new CleanseReportNotificationResult { Success = true, NotificationId = "test-notification-id", Recipient = "test@example.com" });

        var loggerMock = new Mock<ILogger<CleanseReportService>>();

        _sut = new CleanseReportService(
            _queryService,
            _dataSets,
            _reportRepository,
            _analysisRepository,
            _distributedLock,
            exportServiceMock.Object,
            blobStorageServiceFactoryMock.Object,
            notificationServiceMock.Object,
            loggerMock.Object,
            strategies);
    }

    #region StartAnalysisAsync Tests

    [Fact]
    public async Task StartAnalysis_WithNoData_ShouldCompleteSuccessfully()
    {
        // Arrange - No data in collections

        // Act
        var operation = await _sut.StartAnalysisAsync();

        // Assert
        operation.Should().NotBeNull();
        operation!.Status.Should().Be(CleanseAnalysisStatus.Running);

        // Wait for completion
        await WaitForOperationCompletionAsync(operation.Id);

        var completed = await _sut.GetOperationAsync(operation.Id);
        completed.Should().NotBeNull();
        completed!.Status.Should().Be(CleanseAnalysisStatus.Completed);
        completed.RecordsAnalyzed.Should().Be(0);
        completed.IssuesFound.Should().Be(0);
        completed.IssuesResolved.Should().Be(0);

        _testOutputHelper.WriteLine($"Operation completed: {completed.StatusDescription}");
    }

    [Fact]
    public async Task StartAnalysis_WithMatchingCph_ShouldNotCreateIssue()
    {
        // Arrange - CTS record with CPH that exists in SAM with supporting data
        await InsertCtsCphHoldingAsync("AH-12/345/6789", effectiveTo: null);
        await InsertSamCphHoldingAsync("12/345/6789");
        await InsertSamHerdAsync("12/345/6789-001", "PARTY001", null);
        await InsertSamPartyAsync("PARTY001", "test@example.com");

        // Act
        var operation = await _sut.StartAnalysisAsync();
        await WaitForOperationCompletionAsync(operation!.Id);

        // Assert
        var completed = await _sut.GetOperationAsync(operation.Id);
        completed!.Status.Should().Be(CleanseAnalysisStatus.Completed);
        completed.RecordsAnalyzed.Should().Be(1);
        completed.IssuesFound.Should().Be(0);

        var issues = await GetAllCleanseReportItemsAsync();
        issues.Should().BeEmpty();

        _testOutputHelper.WriteLine("No issue created when CPH exists in SAM");
    }

    [Fact]
    public async Task StartAnalysis_WithMissingCph_ShouldCreateIssue()
    {
        // Arrange - CTS record with CPH that does NOT exist in SAM
        await InsertCtsCphHoldingAsync("AH-12/345/6789", effectiveTo: null);
        // No SAM record inserted

        // Act
        var operation = await _sut.StartAnalysisAsync();
        await WaitForOperationCompletionAsync(operation!.Id);

        // Assert
        var completed = await _sut.GetOperationAsync(operation.Id);
        completed!.Status.Should().Be(CleanseAnalysisStatus.Completed);
        completed.RecordsAnalyzed.Should().Be(1);
        completed.IssuesFound.Should().Be(1);

        var issues = await GetAllCleanseReportItemsAsync();
        issues.Should().HaveCount(1);
        issues[0].Code.Should().Be(IssueCodes.CTS_CPH_NOT_IN_SAM);
        issues[0].CtsLidFullIdentifier.Should().Be("AH-12/345/6789");
        issues[0].Cph.Should().Be("12/345/6789");
        issues[0].IsActive.Should().BeTrue();

        _testOutputHelper.WriteLine($"Issue created: {issues[0].Code}");
    }

    [Fact]
    public async Task StartAnalysis_WithInvalidCountyCode_ShouldSkipRecord()
    {
        // Arrange - County code outside valid range (1-51)
        await InsertCtsCphHoldingAsync("AH-99/345/6789", effectiveTo: null); // County 99 is invalid

        // Act
        var operation = await _sut.StartAnalysisAsync();
        await WaitForOperationCompletionAsync(operation!.Id);

        // Assert
        var completed = await _sut.GetOperationAsync(operation.Id);
        completed!.Status.Should().Be(CleanseAnalysisStatus.Completed);
        completed.IssuesFound.Should().Be(0);

        var issues = await GetAllCleanseReportItemsAsync();
        issues.Should().BeEmpty();

        _testOutputHelper.WriteLine("Record with invalid county code was skipped");
    }

    [Fact]
    public async Task StartAnalysis_WithNonAhPrefix_ShouldSkipRecord()
    {
        // Arrange - LID without "AH-" prefix
        await InsertCtsCphHoldingAsync("XX-12/345/6789", effectiveTo: null);

        // Act
        var operation = await _sut.StartAnalysisAsync();
        await WaitForOperationCompletionAsync(operation!.Id);

        // Assert
        var completed = await _sut.GetOperationAsync(operation.Id);
        completed!.Status.Should().Be(CleanseAnalysisStatus.Completed);
        completed.RecordsAnalyzed.Should().Be(0); // Not matched by filter

        _testOutputHelper.WriteLine("Record without AH- prefix was not processed");
    }

    [Fact]
    public async Task StartAnalysis_WithExpiredRecord_ShouldSkipRecord()
    {
        // Arrange - Expired record (LOC_EFFECTIVE_TO in the past)
        var pastDate = DateTime.UtcNow.AddDays(-30);
        await InsertCtsCphHoldingAsync("AH-12/345/6789", effectiveTo: pastDate);

        // Act
        var operation = await _sut.StartAnalysisAsync();
        await WaitForOperationCompletionAsync(operation!.Id);

        // Assert
        var completed = await _sut.GetOperationAsync(operation.Id);
        completed!.Status.Should().Be(CleanseAnalysisStatus.Completed);
        completed.RecordsAnalyzed.Should().Be(1); // Record is fetched but filtered client-side
        completed.IssuesFound.Should().Be(0); // No issue created for expired record

        var issues = await GetAllCleanseReportItemsAsync();
        issues.Should().BeEmpty();

        _testOutputHelper.WriteLine("Expired record was fetched but not processed");
    }

    #endregion

    #region Issue Resolution Tests

    [Fact]
    public async Task StartAnalysis_WhenIssueResolved_ShouldDeactivateIssue()
    {
        // Arrange - First run: Create issue (CPH missing from SAM)
        await InsertCtsCphHoldingAsync("AH-12/345/6789", effectiveTo: null);

        var firstOp = await _sut.StartAnalysisAsync();
        await WaitForOperationCompletionAsync(firstOp!.Id);

        var issuesBefore = await GetAllCleanseReportItemsAsync();
        issuesBefore.Should().HaveCount(1);
        issuesBefore[0].IsActive.Should().BeTrue();

        // Second run: Add SAM record with supporting data (issue should be resolved)
        await InsertSamCphHoldingAsync("12/345/6789");
        await InsertSamHerdAsync("12/345/6789-001", "PARTY001", null);
        await InsertSamPartyAsync("PARTY001", "test@example.com");

        var secondOp = await _sut.StartAnalysisAsync();
        await WaitForOperationCompletionAsync(secondOp!.Id);

        // Assert
        var completed = await _sut.GetOperationAsync(secondOp.Id);
        completed!.IssuesResolved.Should().Be(1);

        var issuesAfter = await GetAllCleanseReportItemsAsync();
        issuesAfter.Should().HaveCount(1);
        issuesAfter[0].IsActive.Should().BeFalse();

        _testOutputHelper.WriteLine("Issue was deactivated when CPH was found in SAM");
    }

    [Fact]
    public async Task StartAnalysis_WhenInactiveIssueReoccurs_ShouldReactivateIssue()
    {
        // Arrange - Create and resolve issue
        await InsertCtsCphHoldingAsync("AH-12/345/6789", effectiveTo: null);

        var firstOp = await _sut.StartAnalysisAsync();
        await WaitForOperationCompletionAsync(firstOp!.Id);

        // Add SAM record with supporting data to resolve
        await InsertSamCphHoldingAsync("12/345/6789");
        await InsertSamHerdAsync("12/345/6789-001", "PARTY001", null);
        await InsertSamPartyAsync("PARTY001", "test@example.com");

        var secondOp = await _sut.StartAnalysisAsync();
        await WaitForOperationCompletionAsync(secondOp!.Id);

        // Delete SAM record (issue reoccurs)
        await DeleteSamCphHoldingAsync("12/345/6789");

        var thirdOp = await _sut.StartAnalysisAsync();
        await WaitForOperationCompletionAsync(thirdOp!.Id);

        // Assert
        var completed = await _sut.GetOperationAsync(thirdOp.Id);
        completed!.IssuesFound.Should().Be(1);

        var issues = await GetAllCleanseReportItemsAsync();
        issues.Should().HaveCount(1);
        issues[0].IsActive.Should().BeTrue();

        _testOutputHelper.WriteLine("Inactive issue was reactivated when CPH disappeared from SAM");
    }

    #endregion

    #region Distributed Lock Tests

    [Fact]
    public async Task StartAnalysis_WhenAlreadyRunning_ShouldReturnNull()
    {
        // Arrange - Insert enough data to make analysis take time
        for (var i = 1; i <= 10; i++)
        {
            await InsertCtsCphHoldingAsync($"AH-{i:D2}/345/6789", effectiveTo: null);
        }

        // Act - Start first analysis
        var firstOp = await _sut.StartAnalysisAsync();
        firstOp.Should().NotBeNull();

        // Immediately try to start second analysis
        var secondOp = await _sut.StartAnalysisAsync();

        // Assert
        secondOp.Should().BeNull("Lock should prevent concurrent analysis");

        // Cleanup - wait for first to complete
        await WaitForOperationCompletionAsync(firstOp!.Id);

        _testOutputHelper.WriteLine("Second analysis correctly blocked by distributed lock");
    }

    [Fact]
    public async Task StartAnalysis_AfterPreviousCompletes_ShouldSucceed()
    {
        // Arrange
        await InsertCtsCphHoldingAsync("AH-12/345/6789", effectiveTo: null);

        // First analysis
        var firstOp = await _sut.StartAnalysisAsync();
        await WaitForOperationCompletionAsync(firstOp!.Id);

        // Act - Second analysis after first completes
        var secondOp = await _sut.StartAnalysisAsync();

        // Assert
        secondOp.Should().NotBeNull();
        await WaitForOperationCompletionAsync(secondOp!.Id);

        var completed = await _sut.GetOperationAsync(secondOp.Id);
        completed!.Status.Should().Be(CleanseAnalysisStatus.Completed);

        _testOutputHelper.WriteLine("Second analysis succeeded after first completed");
    }

    #endregion

    #region Operation Query Tests

    [Fact]
    public async Task GetOperations_ShouldReturnPaginatedResults()
    {
        // Arrange - Run multiple analyses
        for (var i = 0; i < 3; i++)
        {
            var op = await _sut.StartAnalysisAsync();
            await WaitForOperationCompletionAsync(op!.Id);
        }

        // Act
        var operations = await _sut.GetOperationsAsync(skip: 0, top: 10);

        // Assert
        operations.Should().HaveCount(3);
        operations.Should().BeInDescendingOrder(o => o.StartedAtUtc);

        _testOutputHelper.WriteLine($"Retrieved {operations.Count} operations");
    }

    [Fact]
    public async Task GetOperations_WithPagination_ShouldRespectSkipTop()
    {
        // Arrange - Run multiple analyses
        for (var i = 0; i < 5; i++)
        {
            var op = await _sut.StartAnalysisAsync();
            await WaitForOperationCompletionAsync(op!.Id);
        }

        // Act
        var firstPage = await _sut.GetOperationsAsync(skip: 0, top: 2);
        var secondPage = await _sut.GetOperationsAsync(skip: 2, top: 2);

        // Assert
        firstPage.Should().HaveCount(2);
        secondPage.Should().HaveCount(2);
        firstPage.Select(o => o.Id).Should().NotIntersectWith(secondPage.Select(o => o.Id));

        _testOutputHelper.WriteLine("Pagination working correctly");
    }

    [Fact]
    public async Task GetCurrentOperation_WhenRunning_ShouldReturnOperation()
    {
        // Arrange - Insert data to slow down analysis
        for (var i = 1; i <= 5; i++)
        {
            await InsertCtsCphHoldingAsync($"AH-{i:D2}/345/6789", effectiveTo: null);
        }

        // Act
        var startedOp = await _sut.StartAnalysisAsync();
        var currentOp = await _sut.GetCurrentOperationAsync();

        // Assert
        currentOp.Should().NotBeNull();
        currentOp!.Id.Should().Be(startedOp!.Id);
        currentOp.Status.Should().Be(CleanseAnalysisStatus.Running);

        // Cleanup
        await WaitForOperationCompletionAsync(startedOp.Id);

        _testOutputHelper.WriteLine("Current operation retrieved while running");
    }

    [Fact]
    public async Task GetCurrentOperation_WhenNoneRunning_ShouldReturnNull()
    {
        // Act
        var currentOp = await _sut.GetCurrentOperationAsync();

        // Assert
        currentOp.Should().BeNull();

        _testOutputHelper.WriteLine("No current operation when none running");
    }

    #endregion

    #region Progress Tracking Tests

    [Fact]
    public async Task StartAnalysis_ShouldTrackProgress()
    {
        // Arrange - Insert multiple records
        for (var i = 1; i <= 5; i++)
        {
            await InsertCtsCphHoldingAsync($"AH-{i:D2}/345/6789", effectiveTo: null);
        }

        // Act
        var operation = await _sut.StartAnalysisAsync();
        await WaitForOperationCompletionAsync(operation!.Id);

        // Assert
        var completed = await _sut.GetOperationAsync(operation.Id);
        completed.Should().NotBeNull();
        completed!.Status.Should().Be(CleanseAnalysisStatus.Completed);
        completed.ProgressPercentage.Should().Be(100);
        completed.RecordsAnalyzed.Should().Be(5);
        completed.DurationMs.Should().BeGreaterThan(0);
        completed.StatusDescription.Should().Contain("completed");

        _testOutputHelper.WriteLine($"Analysis completed: {completed.RecordsAnalyzed} records in {completed.DurationMs}ms");
    }

    #endregion

    #region Edge Case Tests

    [Fact]
    public async Task StartAnalysis_WithMultipleMissingCphs_ShouldCreateMultipleIssues()
    {
        // Arrange - Multiple CTS records with missing CPHs
        await InsertCtsCphHoldingAsync("AH-12/345/0001", effectiveTo: null);
        await InsertCtsCphHoldingAsync("AH-12/345/0002", effectiveTo: null);
        await InsertCtsCphHoldingAsync("AH-12/345/0003", effectiveTo: null);

        // Act
        var operation = await _sut.StartAnalysisAsync();
        await WaitForOperationCompletionAsync(operation!.Id);

        // Assert
        var completed = await _sut.GetOperationAsync(operation.Id);
        completed!.IssuesFound.Should().Be(3);

        var issues = await GetAllCleanseReportItemsAsync();
        issues.Should().HaveCount(3);
        issues.Should().AllSatisfy(i => i.IsActive.Should().BeTrue());

        _testOutputHelper.WriteLine($"Created {issues.Count} issues for missing CPHs");
    }

    [Fact]
    public async Task StartAnalysis_WithMixedResults_ShouldHandleCorrectly()
    {
        // Arrange - Mix of matching and missing CPHs
        await InsertCtsCphHoldingAsync("AH-12/345/0001", effectiveTo: null);
        await InsertCtsCphHoldingAsync("AH-12/345/0002", effectiveTo: null);
        await InsertCtsCphHoldingAsync("AH-12/345/0003", effectiveTo: null);

        await InsertSamCphHoldingAsync("12/345/0001"); // Match
        await InsertSamCphHoldingAsync("12/345/0003"); // Match
        // 0002 is missing

        // Add SAM Herd and Party for matching records
        await InsertSamHerdAsync("12/345/0001-001", "PARTY001", null);
        await InsertSamHerdAsync("12/345/0003-001", "PARTY003", null);
        await InsertSamPartyAsync("PARTY001", "test1@example.com");
        await InsertSamPartyAsync("PARTY003", "test3@example.com");

        // Act
        var operation = await _sut.StartAnalysisAsync();
        await WaitForOperationCompletionAsync(operation!.Id);

        // Assert
        var completed = await _sut.GetOperationAsync(operation.Id);
        completed!.RecordsAnalyzed.Should().Be(3);
        completed.IssuesFound.Should().Be(1);

        var issues = await GetAllCleanseReportItemsAsync();
        issues.Should().HaveCount(1);
        issues[0].Cph.Should().Be("12/345/0002");

        _testOutputHelper.WriteLine("Mixed results handled correctly");
    }

    [Fact]
    public async Task StartAnalysis_WithBoundaryCountyCodes_ShouldProcessCorrectly()
    {
        // Arrange - Test boundary county codes
        await InsertCtsCphHoldingAsync("AH-01/345/6789", effectiveTo: null); // Min valid
        await InsertCtsCphHoldingAsync("AH-51/345/6789", effectiveTo: null); // Max valid
        await InsertCtsCphHoldingAsync("AH-00/345/6789", effectiveTo: null); // Below min
        await InsertCtsCphHoldingAsync("AH-52/345/6789", effectiveTo: null); // Above max

        // Act
        var operation = await _sut.StartAnalysisAsync();
        await WaitForOperationCompletionAsync(operation!.Id);

        // Assert
        var completed = await _sut.GetOperationAsync(operation.Id);
        completed!.IssuesFound.Should().Be(2); // Only 01 and 51 are valid

        _testOutputHelper.WriteLine("Boundary county codes processed correctly");
    }

    #endregion

    #region Helper Methods

    private const string DateTimeFormat = "yyyy-MM-dd HH:mm:ss";

    private async Task InsertCtsCphHoldingAsync(string lidFullIdentifier, DateTime? effectiveTo)
    {
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>(CtsCphHoldingCollection);

        var document = new BsonDocument
        {
            { "_id", Guid.NewGuid().ToString() },
            { "LID_FULL_IDENTIFIER", lidFullIdentifier },
            { "LOC_EFFECTIVE_TO", effectiveTo.HasValue ? effectiveTo.Value.ToString(DateTimeFormat) : BsonNull.Value },
            { "IsDeleted", false }
        };

        await collection.InsertOneAsync(document);
    }

    private async Task InsertSamCphHoldingAsync(string cph)
    {
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>(SamCphHoldingCollection);

        var document = new BsonDocument
        {
            { "_id", Guid.NewGuid().ToString() },
            { "CPH", cph },
            { "IsDeleted", false }
        };

        await collection.InsertOneAsync(document);
    }

    private async Task InsertSamHerdAsync(string cphh, string? ownerPartyIds, string? keeperPartyIds)
    {
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("sam_herd");

        var document = new BsonDocument
        {
            { "_id", Guid.NewGuid().ToString() },
            { "CPHH", cphh },
            { "OWNER_PARTY_IDS", ownerPartyIds is not null ? BsonValue.Create(ownerPartyIds) : BsonNull.Value },
            { "KEEPER_PARTY_IDS", keeperPartyIds is not null ? BsonValue.Create(keeperPartyIds) : BsonNull.Value },
            { "IsDeleted", false }
        };

        await collection.InsertOneAsync(document);
    }

    private async Task InsertSamPartyAsync(string partyId, string? emailAddress)
    {
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("sam_party");

        var document = new BsonDocument
        {
            { "_id", Guid.NewGuid().ToString() },
            { "PARTY_ID", partyId },
            { "INTERNET_EMAIL_ADDRESS", emailAddress is not null ? BsonValue.Create(emailAddress) : BsonNull.Value },
            { "IsDeleted", false }
        };

        await collection.InsertOneAsync(document);
    }

    private async Task DeleteSamCphHoldingAsync(string cph)
    {
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>(SamCphHoldingCollection);

        await collection.DeleteOneAsync(d => d["CPH"] == cph);
    }

    private async Task<List<CleanseReportItem>> GetAllCleanseReportItemsAsync()
    {
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>(CleanseReportCollection);

        var documents = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();

        return documents.Select(d => new CleanseReportItem
        {
            Id = d["_id"].AsString,
            Code = d["code"].AsString,
            CtsLidFullIdentifier = d["cts_lid_full_identifier"].AsString,
            Cph = d["cph"].AsString,
            CreatedAtUtc = d["created_at"].ToUniversalTime(),
            LastUpdatedAtUtc = d["last_updated_at"].ToUniversalTime(),
            IsActive = d["is_active"].AsBoolean
        }).ToList();
    }

    private async Task WaitForOperationCompletionAsync(string operationId, int maxWaitSeconds = 30)
    {
        var startTime = DateTime.UtcNow;

        while ((DateTime.UtcNow - startTime).TotalSeconds < maxWaitSeconds)
        {
            var operation = await _sut.GetOperationAsync(operationId);

            if (operation?.Status is CleanseAnalysisStatus.Completed or CleanseAnalysisStatus.Failed)
            {
                return;
            }

            await Task.Delay(100);
        }

        throw new TimeoutException($"Operation {operationId} did not complete within {maxWaitSeconds} seconds");
    }

    #endregion
}
