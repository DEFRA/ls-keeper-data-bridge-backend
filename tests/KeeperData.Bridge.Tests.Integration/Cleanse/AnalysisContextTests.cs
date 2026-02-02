using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.Database;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Querying.Impl;
using KeeperData.Core.Querying.Models;
using KeeperData.Core.Reports.Analysis;
using KeeperData.Infrastructure.Database.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Cleanse;

/// <summary>
/// Integration tests for AnalysisContext, specifically testing the scoped cache functionality.
/// </summary>
[Collection("MongoDB"), Trait("Dependence", "docker")]
public class AnalysisContextTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly MongoDbFixture _mongoDbFixture;
    private readonly IMongoClient _mongoClient;
    private readonly string _testDatabaseName = "analysis-context-test-db";

    private IQueryService _queryService = null!;
    private DataSetDefinitions _dataSets = null!;

    private const string TestCollection = "sam_cph_holdings";

    public AnalysisContextTests(
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
    }

    #region Cache Behavior Tests

    [Fact]
    public async Task QueryAsync_SameQueryTwice_ShouldReturnCachedResult()
    {
        // Arrange
        await InsertTestRecordAsync("12/345/0001");
        
        var context = new AnalysisContext("test-operation-1", _queryService, _dataSets);
        var parameters = new QueryParameters
        {
            CollectionName = _dataSets.SamCPHHolding.Name,
            Filter = FilterExpression.Equal("CPH", "12/345/0001"),
            Top = 10
        };

        // Act - First query
        var result1 = await context.QueryAsync(parameters, CancellationToken.None);
        
        // Modify the database between queries
        await InsertTestRecordAsync("12/345/0002");
        
        // Second query with same parameters
        var result2 = await context.QueryAsync(parameters, CancellationToken.None);

        // Assert - Should return cached result, not seeing the new record
        result1.Data.Count.Should().Be(1);
        result2.Data.Count.Should().Be(1);
        result1.Should().BeSameAs(result2, "Cache should return the same instance");

        _testOutputHelper.WriteLine("Cache correctly returned same instance for identical queries");
    }

    [Fact]
    public async Task QueryAsync_DifferentQueries_ShouldNotShareCache()
    {
        // Arrange
        await InsertTestRecordAsync("12/345/0001");
        await InsertTestRecordAsync("12/345/0002");
        
        var context = new AnalysisContext("test-operation-2", _queryService, _dataSets);
        
        var parameters1 = new QueryParameters
        {
            CollectionName = _dataSets.SamCPHHolding.Name,
            Filter = FilterExpression.Equal("CPH", "12/345/0001"),
            Top = 10
        };
        
        var parameters2 = new QueryParameters
        {
            CollectionName = _dataSets.SamCPHHolding.Name,
            Filter = FilterExpression.Equal("CPH", "12/345/0002"),
            Top = 10
        };

        // Act
        var result1 = await context.QueryAsync(parameters1, CancellationToken.None);
        var result2 = await context.QueryAsync(parameters2, CancellationToken.None);

        // Assert
        result1.Should().NotBeSameAs(result2, "Different queries should have different cache entries");
        result1.Data.Count.Should().Be(1);
        result2.Data.Count.Should().Be(1);
        result1.Data[0]["CPH"]?.ToString().Should().Be("12/345/0001");
        result2.Data[0]["CPH"]?.ToString().Should().Be("12/345/0002");

        _testOutputHelper.WriteLine("Different queries correctly use separate cache entries");
    }

    [Fact]
    public async Task QueryAsync_DifferentContexts_ShouldNotShareCache()
    {
        // Arrange
        await InsertTestRecordAsync("12/345/0001");
        
        var context1 = new AnalysisContext("operation-1", _queryService, _dataSets);
        var context2 = new AnalysisContext("operation-2", _queryService, _dataSets);
        
        var parameters = new QueryParameters
        {
            CollectionName = _dataSets.SamCPHHolding.Name,
            Filter = FilterExpression.Equal("CPH", "12/345/0001"),
            Top = 10
        };

        // Act
        var result1 = await context1.QueryAsync(parameters, CancellationToken.None);
        var result2 = await context2.QueryAsync(parameters, CancellationToken.None);

        // Assert - Different contexts should have separate caches
        result1.Should().NotBeSameAs(result2, "Different contexts should have independent caches");
        result1.Data.Count.Should().Be(result2.Data.Count);

        _testOutputHelper.WriteLine("Different analysis contexts have independent caches");
    }

    [Fact]
    public async Task QueryAsync_DifferentSkipValues_ShouldNotShareCache()
    {
        // Arrange
        for (var i = 1; i <= 5; i++)
        {
            await InsertTestRecordAsync($"12/345/000{i}");
        }
        
        var context = new AnalysisContext("test-operation-3", _queryService, _dataSets);
        
        var parameters1 = new QueryParameters
        {
            CollectionName = _dataSets.SamCPHHolding.Name,
            Filter = FilterExpression.Empty(),
            Skip = 0,
            Top = 2
        };
        
        var parameters2 = new QueryParameters
        {
            CollectionName = _dataSets.SamCPHHolding.Name,
            Filter = FilterExpression.Empty(),
            Skip = 2,
            Top = 2
        };

        // Act
        var result1 = await context.QueryAsync(parameters1, CancellationToken.None);
        var result2 = await context.QueryAsync(parameters2, CancellationToken.None);

        // Assert
        result1.Should().NotBeSameAs(result2, "Different skip values should have different cache entries");
        result1.Data.Count.Should().Be(2);
        result2.Data.Count.Should().Be(2);

        _testOutputHelper.WriteLine("Different pagination parameters correctly use separate cache entries");
    }

    [Fact]
    public async Task QueryAsync_DifferentTopValues_ShouldNotShareCache()
    {
        // Arrange
        for (var i = 1; i <= 5; i++)
        {
            await InsertTestRecordAsync($"12/345/000{i}");
        }
        
        var context = new AnalysisContext("test-operation-4", _queryService, _dataSets);
        
        var parameters1 = new QueryParameters
        {
            CollectionName = _dataSets.SamCPHHolding.Name,
            Filter = FilterExpression.Empty(),
            Top = 2
        };
        
        var parameters2 = new QueryParameters
        {
            CollectionName = _dataSets.SamCPHHolding.Name,
            Filter = FilterExpression.Empty(),
            Top = 3
        };

        // Act
        var result1 = await context.QueryAsync(parameters1, CancellationToken.None);
        var result2 = await context.QueryAsync(parameters2, CancellationToken.None);

        // Assert
        result1.Should().NotBeSameAs(result2);
        result1.Data.Count.Should().Be(2);
        result2.Data.Count.Should().Be(3);

        _testOutputHelper.WriteLine("Different top values correctly use separate cache entries");
    }

    [Fact]
    public async Task QuerySingleAsync_ShouldUseCacheCorrectly()
    {
        // Arrange
        await InsertTestRecordAsync("12/345/0001");
        
        var context = new AnalysisContext("test-operation-5", _queryService, _dataSets);
        var parameters = new QueryParameters
        {
            CollectionName = _dataSets.SamCPHHolding.Name,
            Filter = FilterExpression.Equal("CPH", "12/345/0001"),
            Top = 1
        };

        // Act
        var result1 = await context.QuerySingleAsync(parameters, CancellationToken.None);
        
        // Modify data
        await InsertTestRecordAsync("12/345/0002");
        
        var result2 = await context.QuerySingleAsync(parameters, CancellationToken.None);

        // Assert - Should return same cached result
        result1.Should().NotBeNull();
        result2.Should().NotBeNull();
        result1!["CPH"]?.ToString().Should().Be("12/345/0001");
        result2!["CPH"]?.ToString().Should().Be("12/345/0001");

        _testOutputHelper.WriteLine("QuerySingleAsync correctly uses cache");
    }

    [Fact]
    public async Task QuerySingleAsync_WhenNoResults_ShouldReturnNull()
    {
        // Arrange
        var context = new AnalysisContext("test-operation-6", _queryService, _dataSets);
        var parameters = new QueryParameters
        {
            CollectionName = _dataSets.SamCPHHolding.Name,
            Filter = FilterExpression.Equal("CPH", "non-existent"),
            Top = 1
        };

        // Act
        var result = await context.QuerySingleAsync(parameters, CancellationToken.None);

        // Assert
        result.Should().BeNull();

        _testOutputHelper.WriteLine("QuerySingleAsync correctly returns null for no results");
    }

    #endregion

    #region Cache Key Generation Tests

    [Fact]
    public async Task QueryAsync_SameFilterDifferentInstances_ShouldShareCache()
    {
        // Arrange
        await InsertTestRecordAsync("12/345/0001");
        
        var context = new AnalysisContext("test-operation-7", _queryService, _dataSets);
        
        // Create two separate QueryParameters instances with same values
        var parameters1 = new QueryParameters
        {
            CollectionName = _dataSets.SamCPHHolding.Name,
            Filter = FilterExpression.Equal("CPH", "12/345/0001"),
            Top = 10
        };
        
        var parameters2 = new QueryParameters
        {
            CollectionName = _dataSets.SamCPHHolding.Name,
            Filter = FilterExpression.Equal("CPH", "12/345/0001"),
            Top = 10
        };

        // Act
        var result1 = await context.QueryAsync(parameters1, CancellationToken.None);
        var result2 = await context.QueryAsync(parameters2, CancellationToken.None);

        // Assert - Should be same cached instance even though parameters are different objects
        result1.Should().BeSameAs(result2, "Same query parameters should return cached result");

        _testOutputHelper.WriteLine("Cache key generation correctly identifies equivalent queries");
    }

    [Fact]
    public async Task QueryAsync_ComplexFilter_ShouldCacheCorrectly()
    {
        // Arrange
        await InsertTestRecordAsync("12/345/0001");
        
        var context = new AnalysisContext("test-operation-8", _queryService, _dataSets);
        
        var parameters = new QueryParameters
        {
            CollectionName = _dataSets.SamCPHHolding.Name,
            Filter = FilterExpression.And(
                FilterExpression.Equal("CPH", "12/345/0001"),
                FilterExpression.Equal("IsDeleted", false)
            ),
            Top = 10
        };

        // Act
        var result1 = await context.QueryAsync(parameters, CancellationToken.None);
        var result2 = await context.QueryAsync(parameters, CancellationToken.None);

        // Assert
        result1.Should().BeSameAs(result2);

        _testOutputHelper.WriteLine("Complex filters are cached correctly");
    }

    #endregion

    #region Context Properties Tests

    [Fact]
    public void AnalysisContext_ShouldExposeOperationIdAndDataSets()
    {
        // Arrange & Act
        var context = new AnalysisContext("my-operation-id", _queryService, _dataSets);

        // Assert
        context.OperationId.Should().Be("my-operation-id");
        context.DataSets.Should().BeSameAs(_dataSets);

        _testOutputHelper.WriteLine("AnalysisContext correctly exposes OperationId and DataSets");
    }

    #endregion

    #region Concurrent Access Tests

    [Fact]
    public async Task QueryAsync_ConcurrentAccess_ShouldBeSafe()
    {
        // Arrange
        for (var i = 1; i <= 10; i++)
        {
            await InsertTestRecordAsync($"12/345/{i:D4}");
        }
        
        var context = new AnalysisContext("test-operation-9", _queryService, _dataSets);
        var parameters = new QueryParameters
        {
            CollectionName = _dataSets.SamCPHHolding.Name,
            Filter = FilterExpression.Empty(),
            Top = 100
        };

        // Act - Execute multiple concurrent queries
        var tasks = Enumerable.Range(0, 10)
            .Select(_ => context.QueryAsync(parameters, CancellationToken.None))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All results should have same data (cache ensures single query execution)
        var firstResult = results[0];
        foreach (var result in results)
        {
            result.Data.Count.Should().Be(firstResult.Data.Count, "All concurrent queries should return same data");
            result.TotalCount.Should().Be(firstResult.TotalCount);
        }

        _testOutputHelper.WriteLine("Concurrent access to cache is thread-safe");
    }

    #endregion

    #region Helper Methods

    private async Task InsertTestRecordAsync(string cph)
    {
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>(TestCollection);

        var document = new BsonDocument
        {
            { "_id", Guid.NewGuid().ToString() },
            { "CPH", cph },
            { "IsDeleted", false }
        };

        await collection.InsertOneAsync(document);
    }

    #endregion
}
