using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.Database;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Impl;
using KeeperData.Core.Reports.Abstract;
using KeeperData.Core.Reports.Analysis;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Impl;
using KeeperData.Core.Reports.Strategies;
using KeeperData.Infrastructure.Database.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Cleanse;

/// <summary>
/// Integration tests for the CtsSamAnalysisStrategy.
/// </summary>
[Collection("MongoDB"), Trait("Dependence", "docker")]
public class CtsSamAnalysisStrategyTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly MongoDbFixture _mongoDbFixture;
    private readonly IMongoClient _mongoClient;
    private readonly string _testDatabaseName = "strategy-test-db";

    private IQueryService _queryService = null!;
    private DataSetDefinitions _dataSets = null!;
    private ICleanseReportRepository _reportRepository = null!;
    private CtsSamAnalysisStrategy _strategy = null!;

    private const string CtsCphHoldingCollection = "cts_cph_holding";
    private const string SamCphHoldingCollection = "sam_cph_holdings";
    private const string CleanseReportCollection = "cleanse_report";
    private const string DateTimeFormat = "yyyy-MM-dd HH:mm:ss";

    public CtsSamAnalysisStrategyTests(
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
        _strategy = new CtsSamAnalysisStrategy();
    }

    #region Strategy Execution Tests

    [Fact]
    public async Task ExecuteAsync_WithNoRecords_ShouldReturnZeroMetrics()
    {
        // Arrange
        var context = new AnalysisContext("test-op-1", _queryService, _dataSets);
        var issueRecorder = new IssueRecorder(_reportRepository);
        var progressUpdates = new List<(int analyzed, int total, int found, int resolved)>();

        // Act
        var metrics = await _strategy.ExecuteAsync(
            context,
            issueRecorder,
            (analyzed, total, found, resolved) =>
            {
                progressUpdates.Add((analyzed, total, found, resolved));
                return Task.CompletedTask;
            },
            CancellationToken.None);

        // Assert
        metrics.RecordsAnalyzed.Should().Be(0);
        metrics.IssuesFound.Should().Be(0);
        metrics.IssuesResolved.Should().Be(0);

        _testOutputHelper.WriteLine("Strategy correctly handles empty dataset");
    }

    [Fact]
    public async Task ExecuteAsync_WithMatchingRecords_ShouldNotCreateIssues()
    {
        // Arrange
        await InsertCtsCphHoldingAsync("AH-12/345/0001");
        await InsertCtsCphHoldingAsync("AH-12/345/0002");
        await InsertSamCphHoldingAsync("12/345/0001");
        await InsertSamCphHoldingAsync("12/345/0002");
        // Insert SAM Herd and Party with email addresses for the email check rule
        await InsertSamHerdAsync("12/345/0001-001", "PARTY001", null);
        await InsertSamHerdAsync("12/345/0002-001", "PARTY002", null);
        await InsertSamPartyAsync("PARTY001", "test1@example.com");
        await InsertSamPartyAsync("PARTY002", "test2@example.com");

        var context = new AnalysisContext("test-op-2", _queryService, _dataSets);
        var issueRecorder = new IssueRecorder(_reportRepository);

        // Act
        var metrics = await _strategy.ExecuteAsync(
            context,
            issueRecorder,
            (_, _, _, _) => Task.CompletedTask,
            CancellationToken.None);

        // Assert
        metrics.RecordsAnalyzed.Should().Be(2);
        metrics.IssuesFound.Should().Be(0);

        var issues = await GetAllCleanseReportItemsAsync();
        issues.Should().BeEmpty();

        _testOutputHelper.WriteLine("Strategy correctly identifies matching records");
    }

    [Fact]
    public async Task ExecuteAsync_WithMissingRecords_ShouldCreateIssues()
    {
        // Arrange
        await InsertCtsCphHoldingAsync("AH-12/345/0001");
        await InsertCtsCphHoldingAsync("AH-12/345/0002");
        // No SAM records

        var context = new AnalysisContext("test-op-3", _queryService, _dataSets);
        var issueRecorder = new IssueRecorder(_reportRepository);

        // Act
        var metrics = await _strategy.ExecuteAsync(
            context,
            issueRecorder,
            (_, _, _, _) => Task.CompletedTask,
            CancellationToken.None);

        // Assert
        metrics.RecordsAnalyzed.Should().Be(2);
        metrics.IssuesFound.Should().Be(2);

        var issues = await GetAllCleanseReportItemsAsync();
        issues.Should().HaveCount(2);
        issues.Should().AllSatisfy(i =>
        {
            i.Code.Should().Be(IssueCodes.CTS_CPH_NOT_IN_SAM);
            i.IsActive.Should().BeTrue();
        });

        _testOutputHelper.WriteLine($"Strategy created {metrics.IssuesFound} issues for missing SAM records");
    }

    [Fact]
    public async Task ExecuteAsync_WithMixedRecords_ShouldHandleCorrectly()
    {
        // Arrange
        await InsertCtsCphHoldingAsync("AH-12/345/0001"); // Has SAM match
        await InsertCtsCphHoldingAsync("AH-12/345/0002"); // No SAM match
        await InsertCtsCphHoldingAsync("AH-12/345/0003"); // Has SAM match
        await InsertSamCphHoldingAsync("12/345/0001");
        await InsertSamCphHoldingAsync("12/345/0003");
        // Insert SAM Herd and Party for matching records to avoid email address issue
        await InsertSamHerdAsync("12/345/0001-001", "PARTY001", null);
        await InsertSamHerdAsync("12/345/0003-001", "PARTY003", null);
        await InsertSamPartyAsync("PARTY001", "test1@example.com");
        await InsertSamPartyAsync("PARTY003", "test3@example.com");

        var context = new AnalysisContext("test-op-4", _queryService, _dataSets);
        var issueRecorder = new IssueRecorder(_reportRepository);

        // Act
        var metrics = await _strategy.ExecuteAsync(
            context,
            issueRecorder,
            (_, _, _, _) => Task.CompletedTask,
            CancellationToken.None);

        // Assert
        metrics.RecordsAnalyzed.Should().Be(3);
        metrics.IssuesFound.Should().Be(1);

        var issues = await GetAllCleanseReportItemsAsync();
        issues.Should().HaveCount(1);
        issues[0].Cph.Should().Be("12/345/0002");

        _testOutputHelper.WriteLine("Strategy correctly handles mixed matching/missing records");
    }

    #endregion

    #region Issue Resolution Tests

    [Fact]
    public async Task ExecuteAsync_WhenIssueResolved_ShouldDeactivateIssue()
    {
        // Arrange - First run creates issue
        await InsertCtsCphHoldingAsync("AH-12/345/0001");

        var context1 = new AnalysisContext("test-op-5a", _queryService, _dataSets);
        var issueRecorder1 = new IssueRecorder(_reportRepository);

        await _strategy.ExecuteAsync(context1, issueRecorder1, (_, _, _, _) => Task.CompletedTask, CancellationToken.None);

        var issuesBefore = await GetAllCleanseReportItemsAsync();
        issuesBefore.Should().HaveCount(1);
        issuesBefore[0].IsActive.Should().BeTrue();

        // Add SAM record and supporting data
        await InsertSamCphHoldingAsync("12/345/0001");
        await InsertSamHerdAsync("12/345/0001-001", "PARTY001", null);
        await InsertSamPartyAsync("PARTY001", "test@example.com");

        // Second run should resolve issue
        var context2 = new AnalysisContext("test-op-5b", _queryService, _dataSets);
        var issueRecorder2 = new IssueRecorder(_reportRepository);

        var metrics = await _strategy.ExecuteAsync(context2, issueRecorder2, (_, _, _, _) => Task.CompletedTask, CancellationToken.None);

        // Assert
        metrics.IssuesResolved.Should().Be(1);

        var issuesAfter = await GetAllCleanseReportItemsAsync();
        issuesAfter.Should().HaveCount(1);
        issuesAfter[0].IsActive.Should().BeFalse();

        _testOutputHelper.WriteLine("Strategy correctly resolves issues when SAM record appears");
    }

    [Fact]
    public async Task ExecuteAsync_WhenIssueReoccurs_ShouldReactivateIssue()
    {
        // Arrange - Create and resolve issue
        await InsertCtsCphHoldingAsync("AH-12/345/0001");

        var context1 = new AnalysisContext("test-op-6a", _queryService, _dataSets);
        var issueRecorder1 = new IssueRecorder(_reportRepository);
        await _strategy.ExecuteAsync(context1, issueRecorder1, (_, _, _, _) => Task.CompletedTask, CancellationToken.None);

        // Add SAM record and supporting data to resolve the issue
        await InsertSamCphHoldingAsync("12/345/0001");
        await InsertSamHerdAsync("12/345/0001-001", "PARTY001", null);
        await InsertSamPartyAsync("PARTY001", "test@example.com");

        var context2 = new AnalysisContext("test-op-6b", _queryService, _dataSets);
        var issueRecorder2 = new IssueRecorder(_reportRepository);
        await _strategy.ExecuteAsync(context2, issueRecorder2, (_, _, _, _) => Task.CompletedTask, CancellationToken.None);

        // Delete SAM record (issue reoccurs)
        await DeleteSamCphHoldingAsync("12/345/0001");

        var context3 = new AnalysisContext("test-op-6c", _queryService, _dataSets);
        var issueRecorder3 = new IssueRecorder(_reportRepository);

        // Act
        var metrics = await _strategy.ExecuteAsync(context3, issueRecorder3, (_, _, _, _) => Task.CompletedTask, CancellationToken.None);

        // Assert
        metrics.IssuesFound.Should().Be(1, "Reactivated issue should count as found");

        var issues = await GetAllCleanseReportItemsAsync();
        issues.Should().HaveCount(1);
        issues[0].IsActive.Should().BeTrue();

        _testOutputHelper.WriteLine("Strategy correctly reactivates resolved issues");
    }

    #endregion

    #region Filtering Tests

    [Fact]
    public async Task ExecuteAsync_ShouldSkipNonAhPrefixRecords()
    {
        // Arrange
        await InsertCtsCphHoldingAsync("XX-12/345/0001"); // Not AH- prefix

        var context = new AnalysisContext("test-op-7", _queryService, _dataSets);
        var issueRecorder = new IssueRecorder(_reportRepository);

        // Act
        var metrics = await _strategy.ExecuteAsync(context, issueRecorder, (_, _, _, _) => Task.CompletedTask, CancellationToken.None);

        // Assert
        metrics.RecordsAnalyzed.Should().Be(0, "Non-AH records should be filtered by query");
        metrics.IssuesFound.Should().Be(0);

        _testOutputHelper.WriteLine("Strategy correctly filters non-AH prefix records");
    }

    [Fact]
    public async Task ExecuteAsync_ShouldSkipInvalidCountyCodes()
    {
        // Arrange
        await InsertCtsCphHoldingAsync("AH-00/345/0001"); // Below min
        await InsertCtsCphHoldingAsync("AH-52/345/0002"); // Above max
        await InsertCtsCphHoldingAsync("AH-25/345/0003"); // Valid

        var context = new AnalysisContext("test-op-8", _queryService, _dataSets);
        var issueRecorder = new IssueRecorder(_reportRepository);

        // Act
        var metrics = await _strategy.ExecuteAsync(context, issueRecorder, (_, _, _, _) => Task.CompletedTask, CancellationToken.None);

        // Assert
        metrics.IssuesFound.Should().Be(1, "Only valid county code should create issue");

        var issues = await GetAllCleanseReportItemsAsync();
        issues.Should().HaveCount(1);
        issues[0].Cph.Should().Be("25/345/0003");

        _testOutputHelper.WriteLine("Strategy correctly skips invalid county codes");
    }

    [Fact]
    public async Task ExecuteAsync_ShouldSkipExpiredRecords()
    {
        // Arrange
        var pastDate = DateTime.UtcNow.AddDays(-30);
        await InsertCtsCphHoldingAsync("AH-12/345/0001", effectiveTo: pastDate);

        var context = new AnalysisContext("test-op-9", _queryService, _dataSets);
        var issueRecorder = new IssueRecorder(_reportRepository);

        // Act
        var metrics = await _strategy.ExecuteAsync(context, issueRecorder, (_, _, _, _) => Task.CompletedTask, CancellationToken.None);

        // Assert
        metrics.RecordsAnalyzed.Should().Be(1, "Record is fetched but filtered client-side");
        metrics.IssuesFound.Should().Be(0, "Expired record should not create issue");

        _testOutputHelper.WriteLine("Strategy correctly skips expired records");
    }

    [Fact]
    public async Task ExecuteAsync_ShouldSkipDeletedRecords()
    {
        // Arrange
        await InsertCtsCphHoldingAsync("AH-12/345/0001", isDeleted: true);

        var context = new AnalysisContext("test-op-10", _queryService, _dataSets);
        var issueRecorder = new IssueRecorder(_reportRepository);

        // Act
        var metrics = await _strategy.ExecuteAsync(context, issueRecorder, (_, _, _, _) => Task.CompletedTask, CancellationToken.None);

        // Assert
        metrics.RecordsAnalyzed.Should().Be(0, "Deleted records should be filtered by query");

        _testOutputHelper.WriteLine("Strategy correctly filters deleted records");
    }

    #endregion

    #region Progress Callback Tests

    [Fact]
    public async Task ExecuteAsync_ShouldReportProgress()
    {
        // Arrange
        for (var i = 1; i <= 150; i++)
        {
            await InsertCtsCphHoldingAsync($"AH-{(i % 50) + 1:D2}/345/{i:D4}");
        }

        var context = new AnalysisContext("test-op-11", _queryService, _dataSets);
        var issueRecorder = new IssueRecorder(_reportRepository);
        var progressUpdates = new List<(int analyzed, int total, int found, int resolved)>();

        // Act
        var metrics = await _strategy.ExecuteAsync(
            context,
            issueRecorder,
            (analyzed, total, found, resolved) =>
            {
                progressUpdates.Add((analyzed, total, found, resolved));
                _testOutputHelper.WriteLine($"Progress: {analyzed}/{total}, Issues: {found}, Resolved: {resolved}");
                return Task.CompletedTask;
            },
            CancellationToken.None);

        // Assert
        metrics.RecordsAnalyzed.Should().Be(150);
        progressUpdates.Should().NotBeEmpty("Progress should be reported");
        progressUpdates.Should().AllSatisfy(p => p.total.Should().Be(150));

        _testOutputHelper.WriteLine($"Received {progressUpdates.Count} progress updates");
    }

    #endregion

    #region Cache Effectiveness Tests

    [Fact]
    public async Task ExecuteAsync_ShouldBenefitFromCaching()
    {
        // Arrange - Multiple CTS records pointing to different SAM CPHs
        await InsertCtsCphHoldingAsync("AH-12/345/0001");
        await InsertCtsCphHoldingAsync("AH-13/345/0001");
        await InsertCtsCphHoldingAsync("AH-14/345/0001");
        await InsertSamCphHoldingAsync("12/345/0001");
        await InsertSamCphHoldingAsync("13/345/0001");
        await InsertSamCphHoldingAsync("14/345/0001");
        // Insert SAM Herd and Party for all CPHs
        await InsertSamHerdAsync("12/345/0001-001", "PARTY001", null);
        await InsertSamHerdAsync("13/345/0001-001", "PARTY002", null);
        await InsertSamHerdAsync("14/345/0001-001", "PARTY003", null);
        await InsertSamPartyAsync("PARTY001", "test1@example.com");
        await InsertSamPartyAsync("PARTY002", "test2@example.com");
        await InsertSamPartyAsync("PARTY003", "test3@example.com");

        var context = new AnalysisContext("test-op-12", _queryService, _dataSets);
        var issueRecorder = new IssueRecorder(_reportRepository);

        // Act
        var metrics = await _strategy.ExecuteAsync(context, issueRecorder, (_, _, _, _) => Task.CompletedTask, CancellationToken.None);

        // Assert
        metrics.RecordsAnalyzed.Should().Be(3);
        metrics.IssuesFound.Should().Be(0);

        _testOutputHelper.WriteLine("Strategy executes correctly with caching enabled");
    }

    #endregion

    #region Helper Methods

    private async Task InsertCtsCphHoldingAsync(string lidFullIdentifier, DateTime? effectiveTo = null, bool isDeleted = false)
    {
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>(CtsCphHoldingCollection);

        var document = new BsonDocument
        {
            { "_id", Guid.NewGuid().ToString() },
            { "LID_FULL_IDENTIFIER", lidFullIdentifier },
            { "LOC_EFFECTIVE_TO", effectiveTo.HasValue ? effectiveTo.Value.ToString(DateTimeFormat) : BsonNull.Value },
            { "IsDeleted", isDeleted }
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

    #endregion
}
