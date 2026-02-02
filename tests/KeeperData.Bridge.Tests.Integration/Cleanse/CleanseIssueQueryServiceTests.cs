using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.Database;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Querying;
using KeeperData.Core.Reports.Strategies;
using KeeperData.Infrastructure.Database.Configuration;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Cleanse;

/// <summary>
/// Integration tests for CleanseIssueQueryService.
/// </summary>
[Collection("MongoDB"), Trait("Dependence", "docker")]
public class CleanseIssueQueryServiceTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly MongoDbFixture _mongoDbFixture;
    private readonly IMongoClient _mongoClient;
    private readonly string _testDatabaseName = "issue-query-test-db";

    private CleanseIssueQueryService _sut = null!;

    private const string CollectionName = "cleanse_report";

    public CleanseIssueQueryServiceTests(
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

        var mongoConfig = Options.Create<IDatabaseConfig>(new MongoConfig
        {
            DatabaseName = _testDatabaseName,
            DatabaseUri = _mongoDbFixture.ConnectionString
        });

        _sut = new CleanseIssueQueryService(_mongoClient, mongoConfig);
    }

    public async Task DisposeAsync()
    {
        await _mongoClient.DropDatabaseAsync(_testDatabaseName);
        _testOutputHelper.WriteLine($"Cleaned up test database: {_testDatabaseName}");
    }

    #region QueryAsync Tests

    [Fact]
    public async Task QueryAsync_NoFilter_ReturnsAllIssues()
    {
        // Arrange
        await InsertIssueAsync("id1", "CODE_A", "12/345/0001", isActive: true);
        await InsertIssueAsync("id2", "CODE_B", "12/345/0002", isActive: false);

        var query = CleanseIssueQuery.Create();

        // Act
        var result = await _sut.QueryAsync(query);

        // Assert
        result.TotalCount.Should().Be(2);
        result.Items.Should().HaveCount(2);

        _testOutputHelper.WriteLine($"Retrieved {result.Items.Count} of {result.TotalCount} issues");
    }

    [Fact]
    public async Task QueryAsync_ActiveOnly_ReturnsActiveIssues()
    {
        // Arrange
        await InsertIssueAsync("id1", "CODE_A", "12/345/0001", isActive: true);
        await InsertIssueAsync("id2", "CODE_B", "12/345/0002", isActive: false);
        await InsertIssueAsync("id3", "CODE_A", "12/345/0003", isActive: true);

        var query = CleanseIssueQuery.Create().WhereActive();

        // Act
        var result = await _sut.QueryAsync(query);

        // Assert
        result.TotalCount.Should().Be(2);
        result.Items.Should().AllSatisfy(i => i.IsActive.Should().BeTrue());

        _testOutputHelper.WriteLine("Filtered to active issues only");
    }

    [Fact]
    public async Task QueryAsync_InactiveOnly_ReturnsInactiveIssues()
    {
        // Arrange
        await InsertIssueAsync("id1", "CODE_A", "12/345/0001", isActive: true);
        await InsertIssueAsync("id2", "CODE_B", "12/345/0002", isActive: false);

        var query = CleanseIssueQuery.Create().WhereInactive();

        // Act
        var result = await _sut.QueryAsync(query);

        // Assert
        result.TotalCount.Should().Be(1);
        result.Items.Should().AllSatisfy(i => i.IsActive.Should().BeFalse());

        _testOutputHelper.WriteLine("Filtered to inactive issues only");
    }

    [Fact]
    public async Task QueryAsync_ByIssueCode_ReturnsMatchingIssues()
    {
        // Arrange
        await InsertIssueAsync("id1", IssueCodes.CTS_CPH_NOT_IN_SAM, "12/345/0001");
        await InsertIssueAsync("id2", "OTHER_CODE", "12/345/0002");
        await InsertIssueAsync("id3", IssueCodes.CTS_CPH_NOT_IN_SAM, "12/345/0003");

        var query = CleanseIssueQuery.Create().WithIssueCode(IssueCodes.CTS_CPH_NOT_IN_SAM);

        // Act
        var result = await _sut.QueryAsync(query);

        // Assert
        result.TotalCount.Should().Be(2);
        result.Items.Should().AllSatisfy(i => i.Code.Should().Be(IssueCodes.CTS_CPH_NOT_IN_SAM));

        _testOutputHelper.WriteLine($"Filtered by issue code: {IssueCodes.CTS_CPH_NOT_IN_SAM}");
    }

    [Fact]
    public async Task QueryAsync_ByCphContains_ReturnsMatchingIssues()
    {
        // Arrange
        await InsertIssueAsync("id1", "CODE_A", "12/345/0001");
        await InsertIssueAsync("id2", "CODE_A", "12/999/0002");
        await InsertIssueAsync("id3", "CODE_A", "12/345/0003");

        var query = CleanseIssueQuery.Create().WithCphContaining("345");

        // Act
        var result = await _sut.QueryAsync(query);

        // Assert
        result.TotalCount.Should().Be(2);
        result.Items.Should().AllSatisfy(i => i.Cph.Should().Contain("345"));

        _testOutputHelper.WriteLine("Filtered by CPH containing '345'");
    }

    [Fact]
    public async Task QueryAsync_ByCphStartsWith_ReturnsMatchingIssues()
    {
        // Arrange
        await InsertIssueAsync("id1", "CODE_A", "12/345/0001");
        await InsertIssueAsync("id2", "CODE_A", "99/345/0002");
        await InsertIssueAsync("id3", "CODE_A", "12/999/0003");

        var query = CleanseIssueQuery.Create().WithCphStartingWith("12/");

        // Act
        var result = await _sut.QueryAsync(query);

        // Assert
        result.TotalCount.Should().Be(2);
        result.Items.Should().AllSatisfy(i => i.Cph.Should().StartWith("12/"));

        _testOutputHelper.WriteLine("Filtered by CPH starting with '12/'");
    }

    [Fact]
    public async Task QueryAsync_UpdatedAfter_ReturnsRecentlyUpdatedIssues()
    {
        // Arrange
        var oldTime = DateTime.UtcNow.AddDays(-2);
        var recentTime = DateTime.UtcNow.AddHours(-1);
        var cutoffTime = DateTime.UtcNow.AddDays(-1);

        await InsertIssueAsync("id1", "CODE_A", "12/345/0001", lastUpdatedAt: oldTime);
        await InsertIssueAsync("id2", "CODE_A", "12/345/0002", lastUpdatedAt: recentTime);

        var query = CleanseIssueQuery.Create().UpdatedAfter(cutoffTime);

        // Act
        var result = await _sut.QueryAsync(query);

        // Assert
        result.TotalCount.Should().Be(1);
        result.Items[0].Cph.Should().Be("12/345/0002");

        _testOutputHelper.WriteLine("Filtered issues updated after cutoff time");
    }

    [Fact]
    public async Task QueryAsync_CreatedAfter_ReturnsRecentlyCreatedIssues()
    {
        // Arrange
        var oldTime = DateTime.UtcNow.AddDays(-2);
        var recentTime = DateTime.UtcNow.AddHours(-1);
        var cutoffTime = DateTime.UtcNow.AddDays(-1);

        await InsertIssueAsync("id1", "CODE_A", "12/345/0001", createdAt: oldTime);
        await InsertIssueAsync("id2", "CODE_A", "12/345/0002", createdAt: recentTime);

        var query = CleanseIssueQuery.Create().CreatedAfter(cutoffTime);

        // Act
        var result = await _sut.QueryAsync(query);

        // Assert
        result.TotalCount.Should().Be(1);
        result.Items[0].Cph.Should().Be("12/345/0002");

        _testOutputHelper.WriteLine("Filtered issues created after cutoff time");
    }

    [Fact]
    public async Task QueryAsync_CombinedFilters_ReturnsMatchingIssues()
    {
        // Arrange
        var yesterday6pm = DateTime.UtcNow.Date.AddDays(-1).AddHours(18);
        var now = DateTime.UtcNow;

        await InsertIssueAsync("id1", "CODE_A", "12/345/0001", isActive: true, lastUpdatedAt: now);
        await InsertIssueAsync("id2", "CODE_A", "12/345/0002", isActive: false, lastUpdatedAt: now);
        await InsertIssueAsync("id3", "CODE_A", "12/345/0003", isActive: true, lastUpdatedAt: yesterday6pm.AddDays(-1));

        var query = CleanseIssueQuery.Create()
            .WhereActive()
            .UpdatedAfter(yesterday6pm);

        // Act
        var result = await _sut.QueryAsync(query);

        // Assert
        result.TotalCount.Should().Be(1);
        result.Items[0].Id.Should().Be("id1");

        _testOutputHelper.WriteLine("Combined filters work correctly");
    }

    #endregion

    #region Sorting Tests

    [Fact]
    public async Task QueryAsync_SortByCphAscending_ReturnsSortedResults()
    {
        // Arrange
        await InsertIssueAsync("id1", "CODE_A", "99/345/0001");
        await InsertIssueAsync("id2", "CODE_A", "12/345/0002");
        await InsertIssueAsync("id3", "CODE_A", "50/345/0003");

        var query = CleanseIssueQuery.Create().OrderBy(CleanseIssueSortField.Cph, descending: false);

        // Act
        var result = await _sut.QueryAsync(query);

        // Assert
        result.Items[0].Cph.Should().Be("12/345/0002");
        result.Items[1].Cph.Should().Be("50/345/0003");
        result.Items[2].Cph.Should().Be("99/345/0001");

        _testOutputHelper.WriteLine("Sorted by CPH ascending");
    }

    [Fact]
    public async Task QueryAsync_SortByCphDescending_ReturnsSortedResults()
    {
        // Arrange
        await InsertIssueAsync("id1", "CODE_A", "12/345/0001");
        await InsertIssueAsync("id2", "CODE_A", "99/345/0002");
        await InsertIssueAsync("id3", "CODE_A", "50/345/0003");

        var query = CleanseIssueQuery.Create().OrderBy(CleanseIssueSortField.Cph, descending: true);

        // Act
        var result = await _sut.QueryAsync(query);

        // Assert
        result.Items[0].Cph.Should().Be("99/345/0002");
        result.Items[1].Cph.Should().Be("50/345/0003");
        result.Items[2].Cph.Should().Be("12/345/0001");

        _testOutputHelper.WriteLine("Sorted by CPH descending");
    }

    [Fact]
    public async Task QueryAsync_SortByIssueCode_ReturnsSortedResults()
    {
        // Arrange
        await InsertIssueAsync("id1", "CODE_C", "12/345/0001");
        await InsertIssueAsync("id2", "CODE_A", "12/345/0002");
        await InsertIssueAsync("id3", "CODE_B", "12/345/0003");

        var query = CleanseIssueQuery.Create().OrderBy(CleanseIssueSortField.IssueCode, descending: false);

        // Act
        var result = await _sut.QueryAsync(query);

        // Assert
        result.Items[0].Code.Should().Be("CODE_A");
        result.Items[1].Code.Should().Be("CODE_B");
        result.Items[2].Code.Should().Be("CODE_C");

        _testOutputHelper.WriteLine("Sorted by issue code ascending");
    }

    [Fact]
    public async Task QueryAsync_SortByLastUpdated_ReturnsSortedResults()
    {
        // Arrange
        var time1 = DateTime.UtcNow.AddHours(-3);
        var time2 = DateTime.UtcNow.AddHours(-1);
        var time3 = DateTime.UtcNow.AddHours(-2);

        await InsertIssueAsync("id1", "CODE_A", "12/345/0001", lastUpdatedAt: time1);
        await InsertIssueAsync("id2", "CODE_A", "12/345/0002", lastUpdatedAt: time2);
        await InsertIssueAsync("id3", "CODE_A", "12/345/0003", lastUpdatedAt: time3);

        var query = CleanseIssueQuery.Create().OrderBy(CleanseIssueSortField.LastUpdatedAtUtc, descending: true);

        // Act
        var result = await _sut.QueryAsync(query);

        // Assert
        result.Items[0].Id.Should().Be("id2"); // Most recent
        result.Items[1].Id.Should().Be("id3");
        result.Items[2].Id.Should().Be("id1"); // Oldest

        _testOutputHelper.WriteLine("Sorted by last updated descending");
    }

    #endregion

    #region Pagination Tests

    [Fact]
    public async Task QueryAsync_Pagination_ReturnsCorrectPage()
    {
        // Arrange
        for (var i = 1; i <= 10; i++)
        {
            await InsertIssueAsync($"id{i}", "CODE_A", $"12/345/{i:D4}");
        }

        var query = CleanseIssueQuery.Create()
            .OrderBy(CleanseIssueSortField.Cph, descending: false)
            .Page(skip: 3, top: 3);

        // Act
        var result = await _sut.QueryAsync(query);

        // Assert
        result.TotalCount.Should().Be(10);
        result.Items.Should().HaveCount(3);
        result.Skip.Should().Be(3);
        result.Top.Should().Be(3);
        result.HasMore.Should().BeTrue();

        _testOutputHelper.WriteLine($"Page {result.Skip / result.Top + 1}: {result.Items.Count} items, HasMore: {result.HasMore}");
    }

    [Fact]
    public async Task QueryAsync_LastPage_HasMoreIsFalse()
    {
        // Arrange
        for (var i = 1; i <= 5; i++)
        {
            await InsertIssueAsync($"id{i}", "CODE_A", $"12/345/{i:D4}");
        }

        var query = CleanseIssueQuery.Create().Page(skip: 3, top: 10);

        // Act
        var result = await _sut.QueryAsync(query);

        // Assert
        result.TotalCount.Should().Be(5);
        result.Items.Should().HaveCount(2);
        result.HasMore.Should().BeFalse();

        _testOutputHelper.WriteLine("Last page correctly indicates no more items");
    }

    #endregion

    #region CountAsync Tests

    [Fact]
    public async Task CountAsync_ReturnsCorrectCount()
    {
        // Arrange
        await InsertIssueAsync("id1", "CODE_A", "12/345/0001", isActive: true);
        await InsertIssueAsync("id2", "CODE_A", "12/345/0002", isActive: true);
        await InsertIssueAsync("id3", "CODE_A", "12/345/0003", isActive: false);

        var query = CleanseIssueQuery.Create().WhereActive();

        // Act
        var count = await _sut.CountAsync(query);

        // Assert
        count.Should().Be(2);

        _testOutputHelper.WriteLine($"Count of active issues: {count}");
    }

    #endregion

    #region GroupByIssueCodeAsync Tests

    [Fact]
    public async Task GroupByIssueCodeAsync_ReturnsGroupedResults()
    {
        // Arrange
        await InsertIssueAsync("id1", "CODE_A", "12/345/0001");
        await InsertIssueAsync("id2", "CODE_A", "12/345/0002");
        await InsertIssueAsync("id3", "CODE_B", "12/345/0003");
        await InsertIssueAsync("id4", "CODE_A", "12/345/0004");
        await InsertIssueAsync("id5", "CODE_C", "12/345/0005");

        // Act
        var groups = await _sut.GroupByIssueCodeAsync();

        // Assert
        groups.Should().HaveCount(3);
        
        var codeAGroup = groups.First(g => g.IssueCode == "CODE_A");
        codeAGroup.TotalCount.Should().Be(3);
        codeAGroup.Items.Should().HaveCount(3);

        var codeBGroup = groups.First(g => g.IssueCode == "CODE_B");
        codeBGroup.TotalCount.Should().Be(1);

        _testOutputHelper.WriteLine($"Found {groups.Count} groups");
    }

    [Fact]
    public async Task GroupByIssueCodeAsync_WithFilter_ReturnsFilteredGroups()
    {
        // Arrange
        await InsertIssueAsync("id1", "CODE_A", "12/345/0001", isActive: true);
        await InsertIssueAsync("id2", "CODE_A", "12/345/0002", isActive: false);
        await InsertIssueAsync("id3", "CODE_B", "12/345/0003", isActive: true);

        var filter = CleanseIssueQuery.Create().WhereActive();

        // Act
        var groups = await _sut.GroupByIssueCodeAsync(filter);

        // Assert
        groups.Should().HaveCount(2);
        
        var codeAGroup = groups.First(g => g.IssueCode == "CODE_A");
        codeAGroup.TotalCount.Should().Be(1);

        _testOutputHelper.WriteLine("Groups filtered by active status");
    }

    [Fact]
    public async Task GroupByIssueCodeAsync_LimitsItemsPerGroup()
    {
        // Arrange
        for (var i = 1; i <= 15; i++)
        {
            await InsertIssueAsync($"id{i}", "CODE_A", $"12/345/{i:D4}");
        }

        // Act
        var groups = await _sut.GroupByIssueCodeAsync(itemsPerGroup: 5);

        // Assert
        groups.Should().HaveCount(1);
        groups[0].TotalCount.Should().Be(15);
        groups[0].Items.Should().HaveCount(5);

        _testOutputHelper.WriteLine($"Group has {groups[0].TotalCount} total, returned {groups[0].Items.Count} items");
    }

    [Fact]
    public async Task GroupByIssueCodeAsync_SortedByCph_ReturnsItemsSortedWithinGroup()
    {
        // Arrange
        await InsertIssueAsync("id1", "CODE_A", "99/345/0001");
        await InsertIssueAsync("id2", "CODE_A", "12/345/0002");
        await InsertIssueAsync("id3", "CODE_A", "50/345/0003");

        var filter = CleanseIssueQuery.Create().OrderBy(CleanseIssueSortField.Cph, descending: false);

        // Act
        var groups = await _sut.GroupByIssueCodeAsync(filter, itemsPerGroup: 10);

        // Assert
        var group = groups[0];
        group.Items[0].Cph.Should().Be("12/345/0002");
        group.Items[1].Cph.Should().Be("50/345/0003");
        group.Items[2].Cph.Should().Be("99/345/0001");

        _testOutputHelper.WriteLine("Items within group are sorted by CPH");
    }

    [Fact]
    public async Task GroupByIssueCodeAsync_OrderedByCount_ReturnsMostCommonFirst()
    {
        // Arrange
        await InsertIssueAsync("id1", "RARE_CODE", "12/345/0001");
        await InsertIssueAsync("id2", "COMMON_CODE", "12/345/0002");
        await InsertIssueAsync("id3", "COMMON_CODE", "12/345/0003");
        await InsertIssueAsync("id4", "COMMON_CODE", "12/345/0004");

        // Act
        var groups = await _sut.GroupByIssueCodeAsync();

        // Assert
        groups[0].IssueCode.Should().Be("COMMON_CODE");
        groups[0].TotalCount.Should().Be(3);
        groups[1].IssueCode.Should().Be("RARE_CODE");
        groups[1].TotalCount.Should().Be(1);

        _testOutputHelper.WriteLine("Groups ordered by count descending");
    }

    #endregion

    #region Edge Cases

    [Fact]
    public async Task QueryAsync_EmptyCollection_ReturnsEmptyResult()
    {
        // Arrange - no data

        // Act
        var result = await _sut.QueryAsync(CleanseIssueQuery.Create());

        // Assert
        result.TotalCount.Should().Be(0);
        result.Items.Should().BeEmpty();
        result.HasMore.Should().BeFalse();

        _testOutputHelper.WriteLine("Empty collection handled correctly");
    }

    [Fact]
    public async Task GroupByIssueCodeAsync_EmptyCollection_ReturnsEmptyList()
    {
        // Arrange - no data

        // Act
        var groups = await _sut.GroupByIssueCodeAsync();

        // Assert
        groups.Should().BeEmpty();

        _testOutputHelper.WriteLine("Empty collection returns empty groups");
    }

    #endregion

    #region Helper Methods

    private async Task InsertIssueAsync(
        string id,
        string code,
        string cph,
        bool isActive = true,
        DateTime? createdAt = null,
        DateTime? lastUpdatedAt = null)
    {
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>(CollectionName);

        var now = DateTime.UtcNow;
        var document = new BsonDocument
        {
            { "_id", id },
            { "code", code },
            { "cts_lid_full_identifier", $"AH-{cph}" },
            { "cph", cph },
            { "created_at", createdAt ?? now },
            { "last_updated_at", lastUpdatedAt ?? now },
            { "is_active", isActive }
        };

        await collection.InsertOneAsync(document);
    }

    #endregion
}
