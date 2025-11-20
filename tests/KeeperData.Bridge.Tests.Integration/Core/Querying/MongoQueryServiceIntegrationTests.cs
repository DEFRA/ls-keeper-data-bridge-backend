using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.Database;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Querying.Impl;
using KeeperData.Infrastructure.Database.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Core.Querying;

/// <summary>
/// Integration tests for MongoQueryService against real MongoDB using Testcontainers.
/// Tests flexible querying across various operators, data types, and pagination scenarios.
/// </summary>
[Collection("MongoDB"), Trait("Dependence", "docker")]
public class MongoQueryServiceIntegrationTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly MongoDbFixture _mongoDbFixture;
    private readonly IMongoClient _mongoClient;
    private readonly MongoQueryService _queryService;
    private readonly string _testDatabaseName = "test-query-service";
    private readonly string _testCollectionName = "test_products";

    private const int TotalTestRecords = 150; // Enough for pagination tests

    public MongoQueryServiceIntegrationTests(
        ITestOutputHelper testOutputHelper,
        MongoDbFixture mongoDbFixture)
    {
        _testOutputHelper = testOutputHelper;
        _mongoDbFixture = mongoDbFixture;
        _mongoClient = _mongoDbFixture.MongoClient;

        // Setup MongoDB configuration
        var mongoConfig = Options.Create<IDatabaseConfig>(new MongoConfig
        {
            DatabaseName = _testDatabaseName,
            DatabaseUri = _mongoDbFixture.ConnectionString,
            EnableTransactions = false,
            HealthcheckEnabled = false
        });

        // Create mock DataSetDefinitions with our test collection
        var dataSetDefinitions = new DataSetDefinitions
        {
            SamCPHHolding = new DataSetDefinition("sam_cph_holdings", "LITP_SAMCPHHOLDING_{0}", ["CPH"], "CHANGETYPE", []),
            CTSCPHHolding = new DataSetDefinition("sam_cph_holdings", "LITP_SAMCPHHOLDING_{0}", ["CPH"], "CHANGETYPE", []),
            CTSKeeper = new DataSetDefinition(_testCollectionName, "TEST_PRODUCTS_{0}", ["ProductId"], "CHANGETYPE", []),
            SamCPHHolder = new DataSetDefinition(_testCollectionName, "TEST_PRODUCTS_{0}", ["ProductId"], "CHANGETYPE", []),
            SamHerd = new DataSetDefinition(_testCollectionName, "TEST_PRODUCTS_{0}", ["ProductId"], "CHANGETYPE", []),
            SamParty = new DataSetDefinition(_testCollectionName, "TEST_PARTY_{0}", ["PartyId"], "CHANGETYPE", []),
            All =
            [
                new DataSetDefinition("sam_cph_holdings", "LITP_SAMCPHHOLDING_{0}", ["CPH"], "CHANGETYPE", []),
                new DataSetDefinition(_testCollectionName, "TEST_PRODUCTS_{0}", ["ProductId"], "CHANGETYPE", [])
            ]
        };

        var loggerMock = new Mock<ILogger<MongoQueryService>>();

        // Create the service under test
        _queryService = new MongoQueryService(
            _mongoClient,
            mongoConfig,
            dataSetDefinitions,
            loggerMock.Object);
    }

    public async Task InitializeAsync()
    {
        // Clean up database before each test
        await _mongoClient.DropDatabaseAsync(_testDatabaseName);
        _testOutputHelper.WriteLine($"Initialized test database: {_testDatabaseName}");

        // Seed test data
        await SeedTestDataAsync();
    }

    public async Task DisposeAsync()
    {
        await _mongoClient.DropDatabaseAsync(_testDatabaseName);
    }

    private async Task SeedTestDataAsync()
    {
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>(_testCollectionName);

        var documents = new List<BsonDocument>();
        var categories = new[] { "Electronics", "Clothing", "Food", "Books", "Toys" };
        var brands = new[] { "BrandA", "BrandB", "BrandC", "BrandD", "BrandE" };
        var statuses = new[] { "Active", "Discontinued", "OutOfStock" };
        var random = new Random(42); // Fixed seed for reproducibility

        var baseDate = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        for (int i = 1; i <= TotalTestRecords; i++)
        {
            var document = new BsonDocument
            {
                { "_id", $"PROD{i:D4}" },
                { "ProductId", $"PROD{i:D4}" },
                { "Name", $"Product {i}" },
                { "Description", $"Description for product {i} with some searchable text" },
                { "Category", categories[i % categories.Length] },
                { "Brand", brands[i % brands.Length] },
                { "Price", Math.Round(10.0 + (i * 5.5), 2) },
                { "Quantity", i * 10 },
                { "Rating", Math.Round(1.0 + (i % 50) * 0.08, 2) }, // Ratings 1.0 to 5.0
                { "Status", statuses[i % statuses.Length] },
                { "IsDeleted", i % 20 == 0 }, // Every 20th record is deleted
                { "InStock", i % 3 != 0 }, // 2 out of 3 in stock
                { "Featured", i % 10 == 0 }, // Every 10th is featured
                { "CreatedAtUtc", baseDate.AddDays(i) },
                { "UpdatedAtUtc", baseDate.AddDays(i).AddHours(i % 24) },
                { "Tags", new BsonArray { $"tag{i % 5}", $"tag{i % 7}", "common" } },
                { "Metadata", new BsonDocument
                    {
                        { "Supplier", $"Supplier{i % 10}" },
                        { "WarehouseLocation", $"WH-{i % 5}" }
                    }
                }
            };

            documents.Add(document);
        }

        await collection.InsertManyAsync(documents);

        // Create wildcard index for efficient querying
        var indexKeys = Builders<BsonDocument>.IndexKeys.Wildcard();
        var indexModel = new CreateIndexModel<BsonDocument>(indexKeys);
        await collection.Indexes.CreateOneAsync(indexModel);

        _testOutputHelper.WriteLine($"Seeded {TotalTestRecords} test documents with wildcard index");
    }

    #region Basic Query Tests

    [Fact]
    public async Task QueryAsync_WithNoFilters_ReturnsDefaultPageSize()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: null,
            orderBy: null,
            select: null,
            skip: null,
            top: null,
            count: true,
            CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        result.CollectionName.Should().Be(_testCollectionName);
        result.Data.Should().HaveCount(100); // Default page size
        result.Count.Should().Be(100);
        result.TotalCount.Should().Be(TotalTestRecords);
        result.Skip.Should().Be(0);
        result.Top.Should().Be(100);

        _testOutputHelper.WriteLine($"Retrieved {result.Count} records (default page size) out of {result.TotalCount} total");
    }

    [Fact]
    public async Task QueryAsync_WithInvalidCollectionName_ThrowsArgumentException()
    {
        // Act & Assert
        var exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _queryService.QueryAsync(
                "invalid_collection",
                filter: null,
                orderBy: null,
                select: null,
                skip: null,
                top: null,
                count: true,
                CancellationToken.None));

        exception.Message.Should().Contain("not defined in DataSetDefinitions");
        _testOutputHelper.WriteLine($"Correctly rejected invalid collection name");
    }

    #endregion

    #region Comparison Operator Tests

    [Fact]
    public async Task QueryAsync_WithEqualFilter_ReturnsMatchingRecords()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "Category eq 'Electronics'",
            orderBy: null,
            select: null,
            skip: null,
            top: null,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        foreach (var d in result.Data)
        {
            d["Category"]?.ToString().Should().Be("Electronics");
        }
        result.TotalCount.Should().BeGreaterThan(0);

        _testOutputHelper.WriteLine($"Found {result.TotalCount} Electronics products");
    }

    [Fact]
    public async Task QueryAsync_WithNotEqualFilter_ReturnsNonMatchingRecords()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "Category ne 'Electronics'",
            orderBy: null,
            select: null,
            skip: 0,
            top: 50,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        foreach (var d in result.Data)
        {
            d["Category"]?.ToString().Should().NotBe("Electronics");
        }

        _testOutputHelper.WriteLine($"Found {result.Count} non-Electronics products");
    }

    [Fact]
    public async Task QueryAsync_WithGreaterThanFilter_ReturnsMatchingRecords()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "Price gt 500",
            orderBy: null,
            select: null,
            skip: 0,
            top: 50,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        result.Data.Should().OnlyContain(d => Convert.ToDouble(d["Price"]) > 500);

        _testOutputHelper.WriteLine($"Found {result.TotalCount} products with price > 500");
    }

    [Fact]
    public async Task QueryAsync_WithGreaterThanOrEqualFilter_ReturnsMatchingRecords()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "Quantity ge 1000",
            orderBy: null,
            select: null,
            skip: 0,
            top: 50,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        result.Data.Should().OnlyContain(d => Convert.ToInt32(d["Quantity"]) >= 1000);

        _testOutputHelper.WriteLine($"Found {result.TotalCount} products with quantity >= 1000");
    }

    [Fact]
    public async Task QueryAsync_WithLessThanFilter_ReturnsMatchingRecords()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "Rating lt 2.5",
            orderBy: null,
            select: null,
            skip: 0,
            top: 50,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        result.Data.Should().OnlyContain(d => Convert.ToDouble(d["Rating"]) < 2.5);

        _testOutputHelper.WriteLine($"Found {result.TotalCount} products with rating < 2.5");
    }

    [Fact]
    public async Task QueryAsync_WithLessThanOrEqualFilter_ReturnsMatchingRecords()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "Rating le 3.0",
            orderBy: null,
            select: null,
            skip: 0,
            top: 50,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        result.Data.Should().OnlyContain(d => Convert.ToDouble(d["Rating"]) <= 3.0);

        _testOutputHelper.WriteLine($"Found {result.TotalCount} products with rating <= 3.0");
    }

    [Fact]
    public async Task QueryAsync_WithBooleanFilter_ReturnsMatchingRecords()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "IsDeleted eq false",
            orderBy: null,
            select: null,
            skip: 0,
            top: 50,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        foreach (var d in result.Data)
        {
            var isDeleted = d["IsDeleted"];
            isDeleted.Should().BeOfType<bool>();
            if (isDeleted != null)
            {
                ((bool)isDeleted).Should().BeFalse();
            }
        }

        _testOutputHelper.WriteLine($"Found {result.TotalCount} non-deleted products");
    }

    #endregion

    #region Logical Operator Tests

    [Fact]
    public async Task QueryAsync_WithAndOperator_ReturnsMatchingRecords()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "Category eq 'Electronics' and Price gt 300",
            orderBy: null,
            select: null,
            skip: 0,
            top: 50,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        foreach (var d in result.Data)
        {
            d["Category"]?.ToString().Should().Be("Electronics");
            Convert.ToDouble(d["Price"]).Should().BeGreaterThan(300);
        }

        _testOutputHelper.WriteLine($"Found {result.TotalCount} Electronics products with price > 300");
    }

    [Fact]
    public async Task QueryAsync_WithOrOperator_ReturnsMatchingRecords()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "Category eq 'Electronics' or Category eq 'Books'",
            orderBy: null,
            select: null,
            skip: 0,
            top: 50,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        foreach (var d in result.Data)
        {
            var category = d["Category"]?.ToString();
            (category == "Electronics" || category == "Books").Should().BeTrue();
        }

        _testOutputHelper.WriteLine($"Found {result.TotalCount} Electronics or Books products");
    }

    [Fact]
    public async Task QueryAsync_WithComplexLogicalExpression_ReturnsMatchingRecords()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "Category eq 'Electronics' and Price gt 200 and IsDeleted eq false",
            orderBy: null,
            select: null,
            skip: 0,
            top: 50,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        foreach (var d in result.Data)
        {
            d["Category"]?.ToString().Should().Be("Electronics");
            Convert.ToDouble(d["Price"]).Should().BeGreaterThan(200);
            var isDeleted = d["IsDeleted"];
            if (isDeleted != null)
            {
                ((bool)isDeleted).Should().BeFalse();
            }
        }

        _testOutputHelper.WriteLine($"Found {result.TotalCount} active Electronics products with price > 200");
    }

    #endregion

    #region String Function Tests

    [Fact]
    public async Task QueryAsync_WithContainsFunction_ReturnsMatchingRecords()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "contains(Name, 'Product 1')",
            orderBy: null,
            select: null,
            skip: 0,
            top: 50,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        foreach (var d in result.Data)
        {
            var name = d["Name"]?.ToString();
            name.Should().NotBeNull();
            name!.Should().Contain("Product 1");
        }

        _testOutputHelper.WriteLine($"Found {result.TotalCount} products containing 'Product 1'");
    }

    [Fact]
    public async Task QueryAsync_WithStartsWithFunction_ReturnsMatchingRecords()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "startswith(ProductId, 'PROD01')",
            orderBy: null,
            select: null,
            skip: 0,
            top: 50,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        foreach (var d in result.Data)
        {
            var productId = d["ProductId"]?.ToString();
            productId.Should().NotBeNull();
            productId!.Should().StartWith("PROD01");
        }

        _testOutputHelper.WriteLine($"Found {result.TotalCount} products starting with 'PROD01'");
    }

    [Fact]
    public async Task QueryAsync_WithEndsWithFunction_ReturnsMatchingRecords()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "endswith(Brand, 'A')",
            orderBy: null,
            select: null,
            skip: 0,
            top: 50,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        foreach (var d in result.Data)
        {
            var brand = d["Brand"]?.ToString();
            brand.Should().NotBeNull();
            brand!.Should().EndWith("A");
        }

        _testOutputHelper.WriteLine($"Found {result.TotalCount} products with brand ending in 'A'");
    }

    #endregion

    #region Sorting Tests

    [Fact]
    public async Task QueryAsync_WithAscendingSort_ReturnsSortedRecords()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "Category eq 'Electronics'",
            orderBy: "Price asc",
            select: null,
            skip: 0,
            top: 10,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        var prices = result.Data.Select(d => Convert.ToDouble(d["Price"])).ToList();
        prices.Should().BeInAscendingOrder();

        _testOutputHelper.WriteLine($"Retrieved {result.Count} products sorted by price ascending");
    }

    [Fact]
    public async Task QueryAsync_WithDescendingSort_ReturnsSortedRecords()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "IsDeleted eq false",
            orderBy: "UpdatedAtUtc desc",
            select: null,
            skip: 0,
            top: 20,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        var dates = result.Data.Select(d => Convert.ToDateTime(d["UpdatedAtUtc"])).ToList();
        dates.Should().BeInDescendingOrder();

        _testOutputHelper.WriteLine($"Retrieved {result.Count} products sorted by UpdatedAtUtc descending");
    }

    [Fact]
    public async Task QueryAsync_WithMultiFieldSort_ReturnsSortedRecords()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: null,
            orderBy: "Category asc, Price desc",
            select: null,
            skip: 0,
            top: 50,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();

        // Group by category and verify each group is sorted by price descending
        var grouped = result.Data.GroupBy(d => d["Category"]?.ToString());
        foreach (var group in grouped)
        {
            var prices = group.Select(d => Convert.ToDouble(d["Price"])).ToList();
            prices.Should().BeInDescendingOrder($"prices within category '{group.Key}' should be descending");
        }

        _testOutputHelper.WriteLine($"Retrieved {result.Count} products sorted by Category asc, Price desc");
    }

    #endregion

    #region Pagination Tests

    [Fact]
    public async Task QueryAsync_WithSkip_ReturnsCorrectPage()
    {
        // Act - Get first page
        var page1 = await _queryService.QueryAsync(
            _testCollectionName,
            filter: null,
            orderBy: "ProductId asc",
            select: null,
            skip: 0,
            top: 20,
            count: true,
            CancellationToken.None);

        // Act - Get second page
        var page2 = await _queryService.QueryAsync(
            _testCollectionName,
            filter: null,
            orderBy: "ProductId asc",
            select: null,
            skip: 20,
            top: 20,
            count: true,
            CancellationToken.None);

        // Assert
        page1.Data.Should().HaveCount(20);
        page2.Data.Should().HaveCount(20);

        // Verify pages don't overlap by checking product IDs
        var page1Ids = page1.Data.Select(d => d["_id"]?.ToString()).ToHashSet();
        var page2Ids = page2.Data.Select(d => d["_id"]?.ToString()).ToHashSet();
        page1Ids.Intersect(page2Ids).Should().BeEmpty();

        var firstIdPage1 = page1.Data.First()["ProductId"]?.ToString();
        var firstIdPage2 = page2.Data.First()["ProductId"]?.ToString();
        firstIdPage1.Should().NotBe(firstIdPage2);

        _testOutputHelper.WriteLine($"Page 1 first ID: {firstIdPage1}, Page 2 first ID: {firstIdPage2}");
    }

    [Fact]
    public async Task QueryAsync_WithSkipAndTop_CorrectlyPaginates()
    {
        // Arrange
        var pageSize = 25;
        var totalPages = 4;
        var allProductIds = new HashSet<string>();

        // Act - Get multiple pages
        for (int page = 0; page < totalPages; page++)
        {
            var result = await _queryService.QueryAsync(
                _testCollectionName,
                filter: null,
                orderBy: "ProductId asc",
                select: null,
                skip: page * pageSize,
                top: pageSize,
                count: true,
                CancellationToken.None);

            result.Data.Should().HaveCount(pageSize);
            result.Skip.Should().Be(page * pageSize);
            result.Top.Should().Be(pageSize);

            foreach (var doc in result.Data)
            {
                var productId = doc["ProductId"]?.ToString();
                allProductIds.Add(productId!).Should().BeTrue($"ProductId {productId} should not appear in multiple pages");
            }

            _testOutputHelper.WriteLine($"Page {page + 1}: Retrieved {result.Count} records, skip={result.Skip}");
        }

        // Assert
        allProductIds.Should().HaveCount(totalPages * pageSize);
    }

    [Fact]
    public async Task QueryAsync_WithLargeSkip_ReturnsCorrectRecords()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: null,
            orderBy: "ProductId asc",
            select: null,
            skip: 140,
            top: 10,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().HaveCount(10);
        result.Skip.Should().Be(140);
        result.TotalCount.Should().Be(TotalTestRecords);

        _testOutputHelper.WriteLine($"Retrieved last 10 records (skip=140) out of {TotalTestRecords}");
    }

    [Fact]
    public async Task QueryAsync_WithMaxPageSize_CapsAtMaximum()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: null,
            orderBy: null,
            select: null,
            skip: 0,
            top: 5000, // Exceeds max of 1000
            count: true,
            CancellationToken.None);

        // Assert
        result.Top.Should().Be(1000); // Should be capped
        result.Data.Should().HaveCount(TotalTestRecords); // Less than max, so all returned

        _testOutputHelper.WriteLine($"Requested 5000, capped to {result.Top}, returned {result.Count}");
    }

    [Fact]
    public async Task QueryAsync_WithFilterAndPagination_ReturnsCorrectSubset()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "Category eq 'Electronics'",
            orderBy: "Price asc",
            select: null,
            skip: 5,
            top: 10,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().HaveCount(10);
        foreach (var d in result.Data)
        {
            d["Category"]?.ToString().Should().Be("Electronics");
        }
        result.Skip.Should().Be(5);
        result.Top.Should().Be(10);
        result.TotalCount.Should().BeGreaterThan(10);

        var prices = result.Data.Select(d => Convert.ToDouble(d["Price"])).ToList();
        prices.Should().BeInAscendingOrder();

        _testOutputHelper.WriteLine($"Retrieved 10 Electronics products (skip=5) sorted by price, total matching: {result.TotalCount}");
    }

    #endregion

    #region Count Tests

    [Fact]
    public async Task QueryAsync_WithCountTrue_ReturnsTotalCount()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "IsDeleted eq false",
            orderBy: null,
            select: null,
            skip: 0,
            top: 10,
            count: true,
            CancellationToken.None);

        // Assert
        result.TotalCount.Should().NotBeNull();
        result.TotalCount.Should().BeGreaterThan(result.Count);

        _testOutputHelper.WriteLine($"Count: {result.Count}, TotalCount: {result.TotalCount}");
    }

    [Fact]
    public async Task QueryAsync_WithCountFalse_DoesNotReturnTotalCount()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "IsDeleted eq false",
            orderBy: null,
            select: null,
            skip: 0,
            top: 10,
            count: false,
            CancellationToken.None);

        // Assert
        result.TotalCount.Should().BeNull();
        result.Count.Should().Be(10);

        _testOutputHelper.WriteLine($"Count: {result.Count}, TotalCount: null (not requested)");
    }

    #endregion

    #region Data Type Tests

    [Fact]
    public async Task QueryAsync_WithNumericComparison_HandlesIntegers()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "Quantity eq 100",
            orderBy: null,
            select: null,
            skip: 0,
            top: 10,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        result.Data.Should().OnlyContain(d => Convert.ToInt32(d["Quantity"]) == 100);

        _testOutputHelper.WriteLine($"Found {result.TotalCount} products with quantity = 100");
    }

    [Fact]
    public async Task QueryAsync_WithNumericComparison_HandlesDecimals()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "Rating ge 4.0",
            orderBy: null,
            select: null,
            skip: 0,
            top: 20,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        result.Data.Should().OnlyContain(d => Convert.ToDouble(d["Rating"]) >= 4.0);

        _testOutputHelper.WriteLine($"Found {result.TotalCount} products with rating >= 4.0");
    }

    [Fact]
    public async Task QueryAsync_WithDateTimeComparison_HandlesDateFilters()
    {
        // Arrange
        var cutoffDate = new DateTime(2024, 3, 1, 0, 0, 0, DateTimeKind.Utc);
        var cutoffDateString = cutoffDate.ToString("yyyy-MM-ddTHH:mm:ssZ");

        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: $"CreatedAtUtc gt {cutoffDateString}",
            orderBy: null,
            select: null,
            skip: 0,
            top: 50,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        result.Data.Should().OnlyContain(d => Convert.ToDateTime(d["CreatedAtUtc"]) > cutoffDate);

        _testOutputHelper.WriteLine($"Found {result.TotalCount} products created after {cutoffDate:yyyy-MM-dd}");
    }

    #endregion

    #region Complex Scenario Tests

    [Fact]
    public async Task QueryAsync_ComplexFilterWithPaginationAndSorting_ReturnsCorrectResults()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "Category eq 'Electronics' and Price gt 100 and IsDeleted eq false",
            orderBy: "Rating desc, Price asc",
            select: null,
            skip: 5,
            top: 15,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        result.Data.Count.Should().BeLessThanOrEqualTo(15);
        foreach (var d in result.Data)
        {
            d["Category"]?.ToString().Should().Be("Electronics");
            Convert.ToDouble(d["Price"]).Should().BeGreaterThan(100);
            var isDeleted = d["IsDeleted"];
            if (isDeleted != null)
            {
                ((bool)isDeleted).Should().BeFalse();
            }
        }
        result.Skip.Should().Be(5);

        // Verify sorting (descending by rating)
        var ratings = result.Data.Select(d => Convert.ToDouble(d["Rating"])).ToList();
        ratings.Should().BeInDescendingOrder();

        _testOutputHelper.WriteLine($"Complex query returned {result.Count} of {result.TotalCount} matching records");
    }

    [Fact]
    public async Task QueryAsync_WithNestedFieldAccess_ReturnsCorrectResults()
    {
        // Note: This tests that nested documents are converted correctly to dictionaries
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "Category eq 'Electronics'",
            orderBy: null,
            select: null,
            skip: 0,
            top: 5,
            count: false,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        var firstRecord = result.Data.First();
        firstRecord.Should().ContainKey("Metadata");

        var metadata = firstRecord["Metadata"] as Dictionary<string, object?>;
        metadata.Should().NotBeNull();
        metadata!.Should().ContainKey("Supplier");
        metadata.Should().ContainKey("WarehouseLocation");

        _testOutputHelper.WriteLine($"Verified nested document structure in results");
    }

    [Fact]
    public async Task QueryAsync_WithArrayField_ReturnsRecordsWithArrays()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "Featured eq true",
            orderBy: null,
            select: null,
            skip: 0,
            top: 10,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        var firstRecord = result.Data.First();
        firstRecord.Should().ContainKey("Tags");

        var tags = firstRecord["Tags"];
        tags.Should().NotBeNull();
        tags.Should().BeAssignableTo<IEnumerable<object>>();

        _testOutputHelper.WriteLine($"Verified array field handling in results");
    }

    [Fact]
    public async Task QueryAsync_MultipleQueriesInSequence_ReturnConsistentResults()
    {
        // Act - Run same query multiple times
        var results = new List<int>();
        for (int i = 0; i < 3; i++)
        {
            var result = await _queryService.QueryAsync(
                _testCollectionName,
                filter: "Category eq 'Books' and Price lt 200",
                orderBy: "ProductId asc",
                select: null,
                skip: 0,
                top: 50,
                count: true,
                CancellationToken.None);

            results.Add(result.Data.Count);
        }

        // Assert - All queries should return same count
        results.Distinct().Should().HaveCount(1, "all queries should return the same count");
        results[0].Should().Be(results[1]);
        results[1].Should().Be(results[2]);

        _testOutputHelper.WriteLine($"Consistent results across multiple queries: {results[0]} records");
    }

    #endregion

    #region Edge Cases

    [Fact]
    public async Task QueryAsync_WithEmptyResultSet_ReturnsEmptyData()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "ProductId eq 'NONEXISTENT'",
            orderBy: null,
            select: null,
            skip: 0,
            top: 10,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().BeEmpty();
        result.Count.Should().Be(0);
        result.TotalCount.Should().Be(0);

        _testOutputHelper.WriteLine($"Empty result set returned correctly");
    }

    [Fact]
    public async Task QueryAsync_WithZeroTop_ThrowsArgumentException()
    {
        // Act & Assert
        var exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _queryService.QueryAsync(
                _testCollectionName,
                filter: null,
                orderBy: null,
                select: null,
                skip: 0,
                top: 0,
                count: true,
                CancellationToken.None));

        exception.Message.Should().Contain("must be greater than 0");
        _testOutputHelper.WriteLine($"Correctly rejected top=0");
    }

    [Fact]
    public async Task QueryAsync_WithInvalidFilter_ThrowsArgumentException()
    {
        // Act & Assert
        var exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _queryService.QueryAsync(
                _testCollectionName,
                filter: "invalid filter syntax here",
                orderBy: null,
                select: null,
                skip: 0,
                top: 10,
                count: true,
                CancellationToken.None));

        exception.Message.Should().Contain("Invalid filter expression");
        _testOutputHelper.WriteLine($"Correctly rejected invalid filter syntax");
    }

    #endregion

    #region Performance Tests

    [Fact]
    public async Task QueryAsync_WithLargeResultSet_CompletesInReasonableTime()
    {
        // Arrange
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "IsDeleted eq false",
            orderBy: "UpdatedAtUtc desc",
            select: null,
            skip: 0,
            top: 100,
            count: true,
            CancellationToken.None);

        stopwatch.Stop();

        // Assert
        result.Data.Should().HaveCount(100);
        stopwatch.ElapsedMilliseconds.Should().BeLessThan(5000, "Query should complete within 5 seconds");

        _testOutputHelper.WriteLine($"Query completed in {stopwatch.ElapsedMilliseconds}ms");
    }

    #endregion

    #region Select Tests

    [Fact]
    public async Task QueryAsync_WithSelect_ReturnOnlySelectedFields()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "Category eq 'Electronics'",
            orderBy: null,
            select: "ProductId,Name,Price",
            skip: 0,
            top: 10,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        result.Select.Should().Be("ProductId,Name,Price");

        foreach (var record in result.Data)
        {
            record.Keys.Should().HaveCount(3);
            record.Should().ContainKey("ProductId");
            record.Should().ContainKey("Name");
            record.Should().ContainKey("Price");
            record.Should().NotContainKey("Category");
            record.Should().NotContainKey("Description");
            record.Should().NotContainKey("Brand");
        }

        _testOutputHelper.WriteLine($"Retrieved {result.Count} records with only selected fields: ProductId, Name, Price");
    }

    [Fact]
    public async Task QueryAsync_WithSelectSingleField_ReturnsOnlyThatField()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: null,
            orderBy: null,
            select: "ProductId",
            skip: 0,
            top: 5,
            count: false,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();

        foreach (var record in result.Data)
        {
            record.Keys.Should().HaveCount(1);
            record.Should().ContainKey("ProductId");
            record.Should().NotContainKey("Name");
            record.Should().NotContainKey("Price");
        }

        _testOutputHelper.WriteLine($"Retrieved {result.Count} records with only ProductId field");
    }

    [Fact]
    public async Task QueryAsync_WithSelectAndFilterAndSort_CombinesCorrectly()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: "Category eq 'Electronics' and Price gt 200",
            orderBy: "Price asc",
            select: "ProductId,Price,Category",
            skip: 0,
            top: 10,
            count: true,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();

        foreach (var record in result.Data)
        {
            // Only selected fields should be present
            record.Keys.Should().HaveCount(3);
            record.Should().ContainKey("ProductId");
            record.Should().ContainKey("Price");
            record.Should().ContainKey("Category");

            // Filter should still apply
            record["Category"]?.ToString().Should().Be("Electronics");
            Convert.ToDouble(record["Price"]).Should().BeGreaterThan(200);
        }

        // Sort should still apply
        var prices = result.Data.Select(d => Convert.ToDouble(d["Price"])).ToList();
        prices.Should().BeInAscendingOrder();

        _testOutputHelper.WriteLine($"Retrieved {result.Count} filtered and sorted records with selected fields");
    }

    [Fact]
    public async Task QueryAsync_WithSelectNonExistentField_ReturnsEmptyForThatField()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: null,
            orderBy: null,
            select: "ProductId,NonExistentField",
            skip: 0,
            top: 5,
            count: false,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();

        foreach (var record in result.Data)
        {
            record.Should().ContainKey("ProductId");
            record.Should().NotContainKey("NonExistentField");
        }

        _testOutputHelper.WriteLine($"Non-existent fields are excluded from results");
    }

    [Fact]
    public async Task QueryAsync_WithSelectNull_ReturnsAllFields()
    {
        // Act
        var result = await _queryService.QueryAsync(
            _testCollectionName,
            filter: null,
            orderBy: null,
            select: null,
            skip: 0,
            top: 1,
            count: false,
            CancellationToken.None);

        // Assert
        result.Data.Should().NotBeEmpty();
        result.Select.Should().BeNull();

        var firstRecord = result.Data.First();
        firstRecord.Keys.Count.Should().BeGreaterThan(3); // Should have many fields
        firstRecord.Should().ContainKey("ProductId");
        firstRecord.Should().ContainKey("Name");
        firstRecord.Should().ContainKey("Price");
        firstRecord.Should().ContainKey("Category");

        _testOutputHelper.WriteLine($"Without select, all {firstRecord.Keys.Count} fields are returned");
    }

    [Fact]
    public async Task QueryAsync_WithInvalidSelectExpression_ThrowsArgumentException()
    {
        // Act & Assert
        var exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await _queryService.QueryAsync(
                _testCollectionName,
                filter: null,
                orderBy: null,
                select: "Field1,123InvalidField",
                skip: 0,
                top: 10,
                count: true,
                CancellationToken.None));

        exception.Message.Should().Contain("Invalid");
        _testOutputHelper.WriteLine($"Correctly rejected invalid select expression");
    }

    #endregion
}