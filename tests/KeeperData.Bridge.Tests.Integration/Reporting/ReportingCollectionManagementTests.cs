using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.Database;
using KeeperData.Core.Reporting;
using KeeperData.Core.Reporting.Impl;
using KeeperData.Infrastructure.Database.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Reporting;

/// <summary>
/// Integration tests for ReportingCollectionManagementService using TestContainers.MongoDB.
/// Tests the ability to delete reporting and lineage collections and verify auto-recreation.
/// </summary>
[Collection("MongoDB"), Trait("Dependence", "docker")]
public class ReportingCollectionManagementTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly MongoDbFixture _mongoDbFixture;
    private readonly IMongoClient _mongoClient;
    private readonly string _testDatabaseName = "reporting-mgmt-test-db";
    private readonly ReportingCollectionManagementService _sut;
    private readonly Mock<ILogger<ReportingCollectionManagementService>> _loggerMock;

    // Reporting collection names
    private const string ImportReportsCollection = "import_reports";
    private const string ImportFilesCollection = "import_files";
    private const string RecordLineageCollection = "record_lineage";
    private const string RecordLineageEventsCollection = "record_lineage_events";

    public ReportingCollectionManagementTests(
        ITestOutputHelper testOutputHelper,
        MongoDbFixture mongoDbFixture)
    {
        _testOutputHelper = testOutputHelper;
        _mongoDbFixture = mongoDbFixture;
        _mongoClient = _mongoDbFixture.MongoClient;
        _loggerMock = new Mock<ILogger<ReportingCollectionManagementService>>();

        // Setup MongoDB configuration
        var mongoConfig = Options.Create<IDatabaseConfig>(new MongoConfig
        {
            DatabaseName = _testDatabaseName,
            DatabaseUri = _mongoDbFixture.ConnectionString,
            EnableTransactions = false,
            HealthcheckEnabled = false
        });

        // Create the service under test
        _sut = new ReportingCollectionManagementService(
            _mongoClient,
            mongoConfig,
            _loggerMock.Object);
    }

    public async Task InitializeAsync()
    {
        // Clean up database before each test
        await _mongoClient.DropDatabaseAsync(_testDatabaseName);
        _testOutputHelper.WriteLine($"Initialized test database: {_testDatabaseName}");
    }

    public async Task DisposeAsync()
    {
        // Clean up database after each test
        await _mongoClient.DropDatabaseAsync(_testDatabaseName);
        _testOutputHelper.WriteLine($"Cleaned up test database: {_testDatabaseName}");
    }

    [Fact]
    public async Task DeleteReportingCollectionAsync_WithValidCollectionName_ShouldDeleteCollection()
    {
        // Arrange
        await CreateTestCollectionWithDataAsync(ImportReportsCollection);

        // Act
        var result = await _sut.DeleteReportingCollectionAsync(ImportReportsCollection, CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        result.Success.Should().BeTrue();
        result.CollectionName.Should().Be(ImportReportsCollection);
        result.Message.Should().Contain("deleted successfully");
        result.OperatedAtUtc.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));

        // Verify collection no longer exists
        var collectionExists = await CollectionExistsAsync(ImportReportsCollection);
        collectionExists.Should().BeFalse();

        _testOutputHelper.WriteLine($"Successfully deleted collection: {ImportReportsCollection}");
    }

    [Fact]
    public async Task DeleteReportingCollectionAsync_WithInvalidCollectionName_ShouldReturnFailure()
    {
        // Arrange
        var invalidCollectionName = "invalid_collection_name";

        // Act
        var result = await _sut.DeleteReportingCollectionAsync(invalidCollectionName, CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        result.Success.Should().BeFalse();
        result.CollectionName.Should().Be(invalidCollectionName);
        result.Message.Should().Contain("not a reporting collection");
        result.Error.Should().BeOfType<ArgumentException>();

        _testOutputHelper.WriteLine($"Correctly rejected invalid collection name: {invalidCollectionName}");
    }

    [Fact]
    public async Task DeleteReportingCollectionAsync_WithNonExistentCollection_ShouldSucceed()
    {
        // Arrange - Don't create the collection
        var collectionName = ImportReportsCollection;

        // Act
        var result = await _sut.DeleteReportingCollectionAsync(collectionName, CancellationToken.None);

        // Assert - Should still succeed (MongoDB DropCollection is idempotent)
        result.Should().NotBeNull();
        result.Success.Should().BeTrue();
        result.CollectionName.Should().Be(collectionName);

        _testOutputHelper.WriteLine($"Successfully handled deletion of non-existent collection: {collectionName}");
    }

    [Fact]
    public async Task DeleteReportingCollectionAsync_WithCaseInsensitiveMatch_ShouldWork()
    {
        // Arrange
        await CreateTestCollectionWithDataAsync(ImportReportsCollection);

        // Act - Use different casing
        var result = await _sut.DeleteReportingCollectionAsync("IMPORT_REPORTS", CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        result.Success.Should().BeTrue();

        // Verify collection was deleted
        var collectionExists = await CollectionExistsAsync(ImportReportsCollection);
        collectionExists.Should().BeFalse();

        _testOutputHelper.WriteLine("Successfully deleted collection with case-insensitive match");
    }

    [Fact]
    public async Task DeleteAllReportingCollectionsAsync_ShouldDeleteAllFourCollections()
    {
        // Arrange
        await CreateTestCollectionWithDataAsync(ImportReportsCollection);
        await CreateTestCollectionWithDataAsync(ImportFilesCollection);
        await CreateTestCollectionWithDataAsync(RecordLineageCollection);
        await CreateTestCollectionWithDataAsync(RecordLineageEventsCollection);

        // Verify all collections exist
        (await CollectionExistsAsync(ImportReportsCollection)).Should().BeTrue();
        (await CollectionExistsAsync(ImportFilesCollection)).Should().BeTrue();
        (await CollectionExistsAsync(RecordLineageCollection)).Should().BeTrue();
        (await CollectionExistsAsync(RecordLineageEventsCollection)).Should().BeTrue();

        // Act
        var result = await _sut.DeleteAllReportingCollectionsAsync(CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        result.Success.Should().BeTrue();
        result.TotalCount.Should().Be(4);
        result.DeletedCollections.Should().HaveCount(4);
        result.DeletedCollections.Should().Contain(ImportReportsCollection);
        result.DeletedCollections.Should().Contain(ImportFilesCollection);
        result.DeletedCollections.Should().Contain(RecordLineageCollection);
        result.DeletedCollections.Should().Contain(RecordLineageEventsCollection);
        result.Message.Should().Contain("4 reporting collection(s) deleted successfully");

        // Verify all collections no longer exist
        (await CollectionExistsAsync(ImportReportsCollection)).Should().BeFalse();
        (await CollectionExistsAsync(ImportFilesCollection)).Should().BeFalse();
        (await CollectionExistsAsync(RecordLineageCollection)).Should().BeFalse();
        (await CollectionExistsAsync(RecordLineageEventsCollection)).Should().BeFalse();

        _testOutputHelper.WriteLine($"Successfully deleted all {result.TotalCount} reporting collections");
    }

    [Fact]
    public async Task DeleteAllReportingCollectionsAsync_WithSomeNonExistent_ShouldDeleteExistingOnes()
    {
        // Arrange - Only create two of the four collections
        await CreateTestCollectionWithDataAsync(ImportReportsCollection);
        await CreateTestCollectionWithDataAsync(RecordLineageCollection);

        // Act
        var result = await _sut.DeleteAllReportingCollectionsAsync(CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        result.Success.Should().BeTrue();
        result.TotalCount.Should().Be(2); // Only the two that existed
        result.DeletedCollections.Should().HaveCount(2);
        result.DeletedCollections.Should().Contain(ImportReportsCollection);
        result.DeletedCollections.Should().Contain(RecordLineageCollection);

        _testOutputHelper.WriteLine($"Successfully deleted {result.TotalCount} existing collections out of 4 total");
    }

    [Fact]
    public async Task DeleteAllReportingCollectionsAsync_WithNoExistingCollections_ShouldSucceed()
    {
        // Arrange - Don't create any collections

        // Act
        var result = await _sut.DeleteAllReportingCollectionsAsync(CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        result.Success.Should().BeTrue();
        result.TotalCount.Should().Be(0);
        result.DeletedCollections.Should().BeEmpty();
        result.Message.Should().Contain("0 reporting collection(s) deleted successfully");

        _testOutputHelper.WriteLine("Successfully handled deletion with no existing collections");
    }

    [Fact]
    public async Task DeleteReportingCollectionAsync_WithLargeCollection_ShouldDeleteSuccessfully()
    {
        // Arrange - Create collection with many documents
        var collectionName = ImportReportsCollection;
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>(collectionName);

        var documents = Enumerable.Range(1, 1000)
            .Select(i => new BsonDocument { { "_id", i }, { "data", $"test data {i}" } })
            .ToList();

        await collection.InsertManyAsync(documents);

        var count = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        count.Should().Be(1000);

        // Act
        var result = await _sut.DeleteReportingCollectionAsync(collectionName, CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        result.Success.Should().BeTrue();

        var collectionExists = await CollectionExistsAsync(collectionName);
        collectionExists.Should().BeFalse();

        _testOutputHelper.WriteLine($"Successfully deleted large collection with 1000 documents");
    }

    [Fact]
    public async Task AutoRecreation_DeletedCollectionsShouldBeRecreatedWhenDataInserted()
    {
        // Arrange - Create and then delete a collection
        await CreateTestCollectionWithDataAsync(ImportReportsCollection);
        await _sut.DeleteReportingCollectionAsync(ImportReportsCollection, CancellationToken.None);

        var collectionExists = await CollectionExistsAsync(ImportReportsCollection);
        collectionExists.Should().BeFalse("Collection should be deleted");

        // Act - Insert a document (simulating what the ETL process would do)
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>(ImportReportsCollection);
        await collection.InsertOneAsync(new BsonDocument { { "_id", "test" }, { "data", "auto-created" } });

        // Assert - Collection should be auto-created
        collectionExists = await CollectionExistsAsync(ImportReportsCollection);
        collectionExists.Should().BeTrue("Collection should be auto-created");

        var doc = await collection.Find(d => d["_id"] == "test").FirstOrDefaultAsync();
        doc.Should().NotBeNull();
        doc["data"].AsString.Should().Be("auto-created");

        _testOutputHelper.WriteLine("Successfully verified auto-recreation of deleted collection");
    }

    [Fact]
    public async Task DeleteReportingCollectionAsync_WithCancellation_ShouldHandleGracefully()
    {
        // Arrange
        await CreateTestCollectionWithDataAsync(ImportReportsCollection);
        var cts = new CancellationTokenSource();
        cts.Cancel(); // Cancel immediately

        // Act
        var result = await _sut.DeleteReportingCollectionAsync(ImportReportsCollection, cts.Token);

        // Assert
        result.Should().NotBeNull();
        result.Success.Should().BeFalse();
        result.Error.Should().BeOfType<OperationCanceledException>();
        result.Message.Should().Contain("cancelled");

        _testOutputHelper.WriteLine("Successfully handled cancellation");
    }

    [Fact]
    public async Task DeleteAllReportingCollectionsAsync_ConcurrentDeletes_ShouldNotFail()
    {
        // Arrange
        await CreateTestCollectionWithDataAsync(ImportReportsCollection);
        await CreateTestCollectionWithDataAsync(ImportFilesCollection);
        await CreateTestCollectionWithDataAsync(RecordLineageCollection);
        await CreateTestCollectionWithDataAsync(RecordLineageEventsCollection);

        // Act - Delete all collections twice concurrently
        var task1 = _sut.DeleteAllReportingCollectionsAsync(CancellationToken.None);
        var task2 = _sut.DeleteAllReportingCollectionsAsync(CancellationToken.None);

        var results = await Task.WhenAll(task1, task2);

        // Assert - Both should succeed (idempotent)
        results.Should().AllSatisfy(r => r.Success.Should().BeTrue());

        // At least one should have deleted collections
        var totalDeleted = results.Sum(r => r.TotalCount);
        totalDeleted.Should().BeGreaterThanOrEqualTo(4);

        _testOutputHelper.WriteLine($"Successfully handled concurrent deletes, total deleted: {totalDeleted}");
    }

    [Fact]
    public async Task DeleteReportingCollection_AllFourCollectionsIndividually_ShouldWork()
    {
        // Arrange
        var collectionNames = new[]
        {
            ImportReportsCollection,
            ImportFilesCollection,
            RecordLineageCollection,
            RecordLineageEventsCollection
        };

        foreach (var collectionName in collectionNames)
        {
            await CreateTestCollectionWithDataAsync(collectionName);
        }

        // Act & Assert - Delete each collection individually
        foreach (var collectionName in collectionNames)
        {
            var result = await _sut.DeleteReportingCollectionAsync(collectionName, CancellationToken.None);

            result.Should().NotBeNull();
            result.Success.Should().BeTrue();
            result.CollectionName.Should().Be(collectionName);

            var exists = await CollectionExistsAsync(collectionName);
            exists.Should().BeFalse();

            _testOutputHelper.WriteLine($"Successfully deleted collection: {collectionName}");
        }
    }

    /// <summary>
    /// Helper method to create a test collection with sample data
    /// </summary>
    private async Task CreateTestCollectionWithDataAsync(string collectionName)
    {
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>(collectionName);

        var testDocument = new BsonDocument
        {
            { "_id", "test_id" },
            { "testField", "test value" },
            { "createdAt", DateTime.UtcNow }
        };

        await collection.InsertOneAsync(testDocument);

        _testOutputHelper.WriteLine($"Created test collection: {collectionName}");
    }

    /// <summary>
    /// Helper method to check if a collection exists in the database
    /// </summary>
    private async Task<bool> CollectionExistsAsync(string collectionName)
    {
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collections = await database.ListCollectionNamesAsync();
        var collectionList = await collections.ToListAsync();
        return collectionList.Contains(collectionName);
    }
}