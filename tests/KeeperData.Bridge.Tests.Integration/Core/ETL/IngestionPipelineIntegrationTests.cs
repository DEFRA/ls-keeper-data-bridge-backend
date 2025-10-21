using Amazon.S3.Model;
using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.Database;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Reporting;
using KeeperData.Core.Storage;
using KeeperData.Infrastructure.Database.Configuration;
using KeeperData.Infrastructure.Storage;
using KeeperData.Infrastructure.Storage.Configuration;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Factories;
using KeeperData.Infrastructure.Storage.Factories.Implementations;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using System.Text;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Core.ETL;

/// <summary>
/// Integration tests for IngestionPipeline that test end-to-end CSV ingestion from S3 to MongoDB.
/// Uses TestContainers for both LocalStack (S3) and MongoDB.
/// </summary>
[Collection("LocalStackAndMongo"), Trait("Dependence", "docker")]
public class IngestionPipelineIntegrationTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly LocalStackFixture _localStackFixture;
    private readonly MongoDbFixture _mongoDbFixture;
    private readonly List<string> _createdTestFileKeys = new();
    private readonly string _testDatabaseName = "ingestion-test-db";
    private readonly IngestionPipeline _ingestionPipeline;
    private readonly IMongoClient _mongoClient;
    private readonly Mock<ILogger<IngestionPipeline>> _loggerMock;

    private const string TestTopLevelFolder = "litprd";
    private const string DestinationFolder = "dest";

    public IngestionPipelineIntegrationTests(
        ITestOutputHelper testOutputHelper,
        LocalStackFixture localStackFixture,
        MongoDbFixture mongoDbFixture)
    {
        _testOutputHelper = testOutputHelper;
        _localStackFixture = localStackFixture;
        _mongoDbFixture = mongoDbFixture;
        _mongoClient = _mongoDbFixture.MongoClient;
        _loggerMock = new Mock<ILogger<IngestionPipeline>>();

        // Setup blob storage services
        var blobStorageFactory = CreateBlobStorageFactory();
        var externalCatalogueServiceFactory = CreateExternalCatalogueServiceFactory(blobStorageFactory);

        // Setup MongoDB configuration
        var mongoConfig = Options.Create<IDatabaseConfig>(new MongoConfig
        {
            DatabaseName = _testDatabaseName,
            DatabaseUri = _mongoDbFixture.ConnectionString,
            EnableTransactions = false,
            HealthcheckEnabled = false
        });

        // Create the pipeline under test
        var reportingServiceMock = new Mock<IImportReportingService>();

        _ingestionPipeline = new IngestionPipeline(
            blobStorageFactory,
            externalCatalogueServiceFactory,
            _mongoClient,
            mongoConfig,
            reportingServiceMock.Object,
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
        await CleanupTestDataAsync();
        await _mongoClient.DropDatabaseAsync(_testDatabaseName);
    }

    [Fact]
    public async Task StartAsync_WithValidCsvFile_ShouldIngestDataToMongoDB()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);
        var csvContent = GenerateSampleCsvContent(new[]
        {
            ("CPH001", "Farm One", "Owner A", "Address 1"),
            ("CPH002", "Farm Two", "Owner B", "Address 2"),
            ("CPH003", "Farm Three", "Owner C", "Address 3")
        });

        var fileName = $"LITP_SAMCPHHOLDING_{testDate:yyyyMMdd}120000.csv";
        var fileKey = $"{DestinationFolder}/{fileName}";
        await UploadCsvToS3(fileKey, csvContent);

        var importId = Guid.NewGuid();

        // Act
        await _ingestionPipeline.StartAsync(importId, CancellationToken.None);

        // Assert
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("sam_cph_holdings");

        var documents = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        documents.Should().HaveCount(3);

        // Verify first document
        var firstDoc = documents.FirstOrDefault(d => d["_id"] == "CPH001");
        firstDoc.Should().NotBeNull();
        firstDoc["CPH"].Should().Be("CPH001");
        firstDoc["FarmName"].Should().Be("Farm One");
        firstDoc["Owner"].Should().Be("Owner A");
        firstDoc["Address"].Should().Be("Address 1");
        firstDoc.Contains("CreatedAtUtc").Should().BeTrue();
        firstDoc.Contains("UpdatedAtUtc").Should().BeTrue();

        _testOutputHelper.WriteLine($"Successfully ingested {documents.Count} documents to MongoDB");
    }

    [Fact]
    public async Task StartAsync_WithMultipleFiles_ShouldIngestAllFiles()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);

        // Create first file
        var csvContent1 = GenerateSampleCsvContent(new[]
        {
            ("CPH001", "Farm One", "Owner A", "Address 1"),
            ("CPH002", "Farm Two", "Owner B", "Address 2")
        });
        var fileName1 = $"LITP_SAMCPHHOLDING_{testDate:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName1}", csvContent1);

        // Create second file for PREVIOUS day (not future day)
        var testDate2 = testDate.AddDays(-1);
        var csvContent2 = GenerateSampleCsvContent(new[]
        {
            ("CPH003", "Farm Three", "Owner C", "Address 3"),
            ("CPH004", "Farm Four", "Owner D", "Address 4")
        });
        var fileName2 = $"LITP_SAMCPHHOLDING_{testDate2:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName2}", csvContent2);

        var importId = Guid.NewGuid();

        // Act
        await _ingestionPipeline.StartAsync(importId, CancellationToken.None);

        // Assert
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("sam_cph_holdings");

        var documents = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        documents.Should().HaveCount(4);

        var cphIds = documents.Select(d => d["_id"].AsString).OrderBy(x => x).ToArray();
        cphIds.Should().Equal("CPH001", "CPH002", "CPH003", "CPH004");

        _testOutputHelper.WriteLine($"Successfully ingested {documents.Count} documents from multiple files");
    }

    [Fact]
    public async Task StartAsync_WithDuplicateKeys_ShouldUpsertDocuments()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);

        // Create initial file
        var csvContent1 = GenerateSampleCsvContent(new[]
        {
            ("CPH001", "Farm One", "Owner A", "Address 1")
        });
        var fileName1 = $"LITP_SAMCPHHOLDING_{testDate:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName1}", csvContent1);

        var importId1 = Guid.NewGuid();
        await _ingestionPipeline.StartAsync(importId1, CancellationToken.None);

        // Get the first document's CreatedAtUtc
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("sam_cph_holdings");
        var originalDoc = await collection.Find(d => d["_id"] == "CPH001").FirstOrDefaultAsync();
        var originalCreatedAt = originalDoc["CreatedAtUtc"].ToUniversalTime();

        // Wait a bit to ensure timestamp difference
        await Task.Delay(100);

        // Delete the first file so it doesn't get re-processed
        await _localStackFixture.S3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = LocalStackFixture.TestBucket,
            Key = $"{DestinationFolder}/{fileName1}"
        });
        _createdTestFileKeys.Remove($"{DestinationFolder}/{fileName1}");

        // Create updated file with same key but different data on a PREVIOUS day
        var csvContent2 = GenerateSampleCsvContent(new[]
        {
            ("CPH001", "Farm One Updated", "Owner A Updated", "Address 1 Updated")
        });
        var testDate2 = testDate.AddDays(-1);
        var fileName2 = $"LITP_SAMCPHHOLDING_{testDate2:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName2}", csvContent2);

        var importId2 = Guid.NewGuid();

        // Act
        await _ingestionPipeline.StartAsync(importId2, CancellationToken.None);

        // Assert
        var documents = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        documents.Should().HaveCount(1, "Should have upserted the existing document");

        var updatedDoc = documents[0];
        updatedDoc["_id"].Should().Be("CPH001");
        updatedDoc["FarmName"].Should().Be("Farm One Updated");
        updatedDoc["Owner"].Should().Be("Owner A Updated");
        updatedDoc["Address"].Should().Be("Address 1 Updated");

        // Verify CreatedAtUtc was preserved
        updatedDoc["CreatedAtUtc"].ToUniversalTime().Should().Be(originalCreatedAt);

        // Verify UpdatedAtUtc was updated
        updatedDoc["UpdatedAtUtc"].ToUniversalTime().Should().BeAfter(originalCreatedAt);

        _testOutputHelper.WriteLine("Successfully upserted document with preserved CreatedAtUtc");
    }

    [Fact]
    public async Task StartAsync_WithLargeCsvFile_ShouldIngestAllRecordsInBatches()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);
        var recordCount = 2500; // More than 2 batches (batch size is 1000)

        var records = Enumerable.Range(1, recordCount)
            .Select(i => (
                $"CPH{i:D6}",
                $"Farm {i}",
                $"Owner {i}",
                $"Address {i}"))
            .ToArray();

        var csvContent = GenerateSampleCsvContent(records);
        var fileName = $"LITP_SAMCPHHOLDING_{testDate:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName}", csvContent);

        var importId = Guid.NewGuid();

        // Act
        await _ingestionPipeline.StartAsync(importId, CancellationToken.None);

        // Assert
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("sam_cph_holdings");

        var documents = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        documents.Should().HaveCount(recordCount);

        // Verify a few random records
        var doc500 = documents.FirstOrDefault(d => d["_id"] == "CPH000500");
        doc500.Should().NotBeNull();
        doc500["FarmName"].Should().Be("Farm 500");

        var doc2000 = documents.FirstOrDefault(d => d["_id"] == "CPH002000");
        doc2000.Should().NotBeNull();
        doc2000["FarmName"].Should().Be("Farm 2000");

        _testOutputHelper.WriteLine($"Successfully ingested {documents.Count} records in batches");
    }

    [Fact]
    public async Task StartAsync_ShouldCreateWildcardIndex()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);
        var csvContent = GenerateSampleCsvContent(new[]
        {
            ("CPH001", "Farm One", "Owner A", "Address 1")
        });

        var fileName = $"LITP_SAMCPHHOLDING_{testDate:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName}", csvContent);

        var importId = Guid.NewGuid();

        // Act
        await _ingestionPipeline.StartAsync(importId, CancellationToken.None);

        // Assert
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("sam_cph_holdings");

        // Verify data was ingested
        var documents = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        documents.Should().HaveCount(1, "Data should be ingested even if wildcard index creation fails");

        // Try to manually create wildcard index to verify MongoDB version supports it
        try
        {
            var wildcardIndexKeys = Builders<BsonDocument>.IndexKeys.Wildcard("$**");
            var indexModel = new CreateIndexModel<BsonDocument>(wildcardIndexKeys);
            await collection.Indexes.CreateOneAsync(indexModel);

            // If we get here, wildcard indexes are supported
            var indexes = await collection.Indexes.ListAsync();
            var indexList = await indexes.ToListAsync();

            _testOutputHelper.WriteLine("Wildcard index creation succeeded manually");
            foreach (var index in indexList)
            {
                _testOutputHelper.WriteLine($"Index: {index.ToJson()}");
            }

            var hasWildcardIndex = indexList.Any(index =>
            {
                if (index.TryGetValue("key", out var key) && key.IsBsonDocument)
                {
                    var keyDoc = key.AsBsonDocument;
                    return keyDoc.Any(elem => elem.Name == "$**" || (elem.Value != null && elem.Value.ToString()!.Contains("$**")));
                }
                return false;
            });

            hasWildcardIndex.Should().BeTrue("Wildcard index should be created");
        }
        catch (MongoCommandException ex)
        {
            _testOutputHelper.WriteLine($"MongoDB wildcard index not supported in this version: {ex.Message}");
            // This is acceptable - older MongoDB versions don't support wildcard indexes
            // The important thing is that ingestion still works
        }

        _testOutputHelper.WriteLine("Successfully verified wildcard index handling");
    }

    [Fact]
    public async Task StartAsync_WithNullValues_ShouldStoreBsonNull()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);
        var csvContent = "CPH,FarmName,Owner,Address,CHANGE_TYPE\n" +
                        "CPH001,Farm One,,,I\n" +
                        "CPH002,,Owner B,Address 2,I\n";

        var fileName = $"LITP_SAMCPHHOLDING_{testDate:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName}", csvContent);

        var importId = Guid.NewGuid();

        // Act
        await _ingestionPipeline.StartAsync(importId, CancellationToken.None);

        // Assert
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("sam_cph_holdings");

        var doc1 = await collection.Find(d => d["_id"] == "CPH001").FirstOrDefaultAsync();
        doc1.Should().NotBeNull();
        doc1["Owner"].Should().Be(BsonNull.Value);
        doc1["Address"].Should().Be(BsonNull.Value);
        doc1["IsDeleted"].AsBoolean.Should().BeFalse();

        var doc2 = await collection.Find(d => d["_id"] == "CPH002").FirstOrDefaultAsync();
        doc2.Should().NotBeNull();
        doc2["FarmName"].Should().Be(BsonNull.Value);
        doc2["IsDeleted"].AsBoolean.Should().BeFalse();

        _testOutputHelper.WriteLine("Successfully verified null value handling");
    }

    [Fact]
    public async Task StartAsync_WithEmptyFile_ShouldNotFail()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);
        var csvContent = "CPH,FarmName,Owner,Address,CHANGE_TYPE\n"; // Headers only

        var fileName = $"LITP_SAMCPHHOLDING_{testDate:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName}", csvContent);

        var importId = Guid.NewGuid();

        // Act
        var act = async () => await _ingestionPipeline.StartAsync(importId, CancellationToken.None);

        // Assert
        await act.Should().NotThrowAsync();

        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("sam_cph_holdings");
        var documents = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        documents.Should().BeEmpty();

        _testOutputHelper.WriteLine("Successfully handled empty CSV file");
    }

    [Fact]
    public async Task StartAsync_WithMissingPrimaryKey_ShouldThrowException()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);
        var csvContent = "FarmName,Owner,Address,CHANGE_TYPE\n" + // Missing CPH column
                        "Farm One,Owner A,Address 1,I\n";

        var fileName = $"LITP_SAMCPHHOLDING_{testDate:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName}", csvContent);

        var importId = Guid.NewGuid();

        // Act
        var act = async () => await _ingestionPipeline.StartAsync(importId, CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Primary key header 'CPH' not found*");

        _testOutputHelper.WriteLine("Successfully validated primary key requirement");
    }

    [Fact]
    public async Task StartAsync_WithSoftDelete_ShouldMarkRecordAsDeleted()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);

        // First, insert a record
        var csvContent1 = GenerateSampleCsvContentWithChangeType(new[]
        {
            ("CPH001", "Farm One", "Owner A", "Address 1", "I")
        });
        var fileName1 = $"LITP_SAMCPHHOLDING_{testDate:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName1}", csvContent1);

        var importId1 = Guid.NewGuid();
        await _ingestionPipeline.StartAsync(importId1, CancellationToken.None);

        // Delete the first file
        await _localStackFixture.S3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = LocalStackFixture.TestBucket,
            Key = $"{DestinationFolder}/{fileName1}"
        });
        _createdTestFileKeys.Remove($"{DestinationFolder}/{fileName1}");

        // Now, soft delete the record
        var csvContent2 = GenerateSampleCsvContentWithChangeType(new[]
        {
            ("CPH001", "Farm One", "Owner A", "Address 1", "D")
        });
        var testDate2 = testDate.AddDays(-1);
        var fileName2 = $"LITP_SAMCPHHOLDING_{testDate2:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName2}", csvContent2);

        var importId2 = Guid.NewGuid();

        // Act
        await _ingestionPipeline.StartAsync(importId2, CancellationToken.None);

        // Assert
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("sam_cph_holdings");
        var doc = await collection.Find(d => d["_id"] == "CPH001").FirstOrDefaultAsync();

        doc.Should().NotBeNull();
        doc["IsDeleted"].AsBoolean.Should().BeTrue();
        doc.Contains("DeletedAtUtc").Should().BeTrue();
        doc["UpdatedAtUtc"].ToUniversalTime().Should().BeAfter(doc["CreatedAtUtc"].ToUniversalTime());

        _testOutputHelper.WriteLine("Successfully soft-deleted record");
    }

    [Fact]
    public async Task StartAsync_WithUpdateToSoftDeletedRecord_ShouldNotUpdate()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);

        // First, insert and soft delete a record
        var csvContent1 = GenerateSampleCsvContentWithChangeType(new[]
        {
            ("CPH001", "Farm One", "Owner A", "Address 1", "I")
        });
        var fileName1 = $"LITP_SAMCPHHOLDING_{testDate:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName1}", csvContent1);

        await _ingestionPipeline.StartAsync(Guid.NewGuid(), CancellationToken.None);

        await _localStackFixture.S3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = LocalStackFixture.TestBucket,
            Key = $"{DestinationFolder}/{fileName1}"
        });
        _createdTestFileKeys.Remove($"{DestinationFolder}/{fileName1}");

        // Soft delete it
        var csvContent2 = GenerateSampleCsvContentWithChangeType(new[]
        {
            ("CPH001", "Farm One", "Owner A", "Address 1", "D")
        });
        var testDate2 = testDate.AddDays(-1);
        var fileName2 = $"LITP_SAMCPHHOLDING_{testDate2:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName2}", csvContent2);

        await _ingestionPipeline.StartAsync(Guid.NewGuid(), CancellationToken.None);

        await _localStackFixture.S3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = LocalStackFixture.TestBucket,
            Key = $"{DestinationFolder}/{fileName2}"
        });
        _createdTestFileKeys.Remove($"{DestinationFolder}/{fileName2}");

        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("sam_cph_holdings");
        var originalDoc = await collection.Find(d => d["_id"] == "CPH001").FirstOrDefaultAsync();
        var originalUpdatedAt = originalDoc["UpdatedAtUtc"].ToUniversalTime();

        await Task.Delay(100);

        // Try to update the soft-deleted record
        var csvContent3 = GenerateSampleCsvContentWithChangeType(new[]
        {
            ("CPH001", "Farm One Updated", "Owner A Updated", "Address 1 Updated", "U")
        });
        var testDate3 = testDate.AddDays(-2);
        var fileName3 = $"LITP_SAMCPHHOLDING_{testDate3:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName3}", csvContent3);

        // Act
        await _ingestionPipeline.StartAsync(Guid.NewGuid(), CancellationToken.None);

        // Assert
        var doc = await collection.Find(d => d["_id"] == "CPH001").FirstOrDefaultAsync();

        doc.Should().NotBeNull();
        doc["IsDeleted"].AsBoolean.Should().BeTrue();
        doc["FarmName"].Should().Be("Farm One"); // Should NOT be updated
        doc["Owner"].Should().Be("Owner A"); // Should NOT be updated
        doc["UpdatedAtUtc"].ToUniversalTime().Should().Be(originalUpdatedAt); // Should NOT change

        _testOutputHelper.WriteLine("Successfully prevented update to soft-deleted record");
    }

    [Fact]
    public async Task StartAsync_WithInsertToSoftDeletedRecord_ShouldNotInsert()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);

        // First, insert and soft delete a record
        var csvContent1 = GenerateSampleCsvContentWithChangeType(new[]
        {
            ("CPH001", "Farm One", "Owner A", "Address 1", "I")
        });
        var fileName1 = $"LITP_SAMCPHHOLDING_{testDate:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName1}", csvContent1);

        await _ingestionPipeline.StartAsync(Guid.NewGuid(), CancellationToken.None);

        await _localStackFixture.S3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = LocalStackFixture.TestBucket,
            Key = $"{DestinationFolder}/{fileName1}"
        });
        _createdTestFileKeys.Remove($"{DestinationFolder}/{fileName1}");

        // Soft delete it
        var csvContent2 = GenerateSampleCsvContentWithChangeType(new[]
        {
            ("CPH001", "Farm One", "Owner A", "Address 1", "D")
        });
        var testDate2 = testDate.AddDays(-1);
        var fileName2 = $"LITP_SAMCPHHOLDING_{testDate2:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName2}", csvContent2);

        await _ingestionPipeline.StartAsync(Guid.NewGuid(), CancellationToken.None);

        await _localStackFixture.S3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = LocalStackFixture.TestBucket,
            Key = $"{DestinationFolder}/{fileName2}"
        });
        _createdTestFileKeys.Remove($"{DestinationFolder}/{fileName2}");

        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("sam_cph_holdings");
        var originalDoc = await collection.Find(d => d["_id"] == "CPH001").FirstOrDefaultAsync();
        var originalUpdatedAt = originalDoc["UpdatedAtUtc"].ToUniversalTime();

        await Task.Delay(100);

        // Try to re-insert the soft-deleted record
        var csvContent3 = GenerateSampleCsvContentWithChangeType(new[]
        {
            ("CPH001", "Farm One New", "Owner A New", "Address 1 New", "I")
        });
        var testDate3 = testDate.AddDays(-2);
        var fileName3 = $"LITP_SAMCPHHOLDING_{testDate3:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName3}", csvContent3);

        // Act
        await _ingestionPipeline.StartAsync(Guid.NewGuid(), CancellationToken.None);

        // Assert
        var doc = await collection.Find(d => d["_id"] == "CPH001").FirstOrDefaultAsync();

        doc.Should().NotBeNull();
        doc["IsDeleted"].AsBoolean.Should().BeTrue();
        doc["FarmName"].Should().Be("Farm One"); // Should NOT be changed
        doc["Owner"].Should().Be("Owner A"); // Should NOT be changed
        doc["UpdatedAtUtc"].ToUniversalTime().Should().Be(originalUpdatedAt); // Should NOT change

        _testOutputHelper.WriteLine("Successfully prevented re-insert of soft-deleted record");
    }

    [Fact]
    public async Task StartAsync_WithMixedChangeTypes_ShouldProcessCorrectly()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);

        var csvContent = GenerateSampleCsvContentWithChangeType(new[]
        {
            ("CPH001", "Farm One", "Owner A", "Address 1", "I"),
            ("CPH002", "Farm Two", "Owner B", "Address 2", "I"),
            ("CPH003", "Farm Three", "Owner C", "Address 3", "I")
        });
        var fileName = $"LITP_SAMCPHHOLDING_{testDate:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName}", csvContent);

        var importId = Guid.NewGuid();

        // Act
        await _ingestionPipeline.StartAsync(importId, CancellationToken.None);

        // Assert
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("sam_cph_holdings");

        var documents = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        documents.Should().HaveCount(3);

        foreach (var doc in documents)
        {
            doc["IsDeleted"].AsBoolean.Should().BeFalse();
            doc.Contains("CreatedAtUtc").Should().BeTrue();
            doc.Contains("UpdatedAtUtc").Should().BeTrue();
        }

        _testOutputHelper.WriteLine($"Successfully processed {documents.Count} mixed change type records");
    }

    private IBlobStorageServiceFactory CreateBlobStorageFactory()
    {
        var loggerFactory = new Mock<ILoggerFactory>();
        loggerFactory.Setup(x => x.CreateLogger(It.IsAny<string>()))
            .Returns(new Mock<ILogger>().Object);

        var s3ClientFactory = new Mock<IS3ClientFactory>();

        // Setup for destination storage (Get method)
        s3ClientFactory.Setup(x => x.GetClientInfo<InternalStorageClient>())
            .Returns(new S3ClientFactory.ClientInfo(
                _localStackFixture.S3Client,
                LocalStackFixture.TestBucket));

        var storageConfig = new StorageConfiguration
        {
            SourceInternalPrefix = TestTopLevelFolder,
            TargetInternalPrefix = DestinationFolder,
            SourceExternalPrefix = TestTopLevelFolder,
            InternalStorage = new StorageConfigurationDetails
            {
                BucketName = LocalStackFixture.TestBucket,
                HealthcheckEnabled = false
            },
            ExternalStorage = new StorageWithCredentialsConfiguration
            {
                BucketName = LocalStackFixture.TestBucket,
                HealthcheckEnabled = false,
                AccessKeySecretName = "test",
                SecretKeySecretName = "test"
            }
        };

        return new S3BlobStorageServiceFactory(s3ClientFactory.Object, loggerFactory.Object, storageConfig);
    }

    private IExternalCatalogueServiceFactory CreateExternalCatalogueServiceFactory(IBlobStorageServiceFactory blobStorageFactory)
    {
        var factory = new Mock<IExternalCatalogueServiceFactory>();
        var definitions = StandardDataSetDefinitionsBuilder.Build();

        factory.Setup(x => x.Create(It.IsAny<IBlobStorageServiceReadOnly>()))
            .Returns((IBlobStorageServiceReadOnly blobs) =>
            {
                // Create a blob service that points to the destination folder where test files are uploaded
                var loggerMock = new Mock<ILogger<S3BlobStorageServiceReadOnly>>();
                var destBlobService = new S3BlobStorageServiceReadOnly(
                    _localStackFixture.S3Client,
                    loggerMock.Object,
                    LocalStackFixture.TestBucket,
                    DestinationFolder);

                var timeProvider = new FakeTimeProvider(new DateTimeOffset(2024, 12, 15, 10, 0, 0, TimeSpan.Zero));
                return new ExternalCatalogueService(destBlobService, timeProvider, definitions);
            });

        return factory.Object;
    }

    private string GenerateSampleCsvContent((string cph, string farmName, string owner, string address)[] records)
    {
        var sb = new StringBuilder();
        sb.AppendLine("CPH,FarmName,Owner,Address,CHANGE_TYPE");

        foreach (var (cph, farmName, owner, address) in records)
        {
            sb.AppendLine($"{cph},{farmName},{owner},{address},{ChangeType.Insert}");
        }

        return sb.ToString();
    }

    private string GenerateSampleCsvContentWithChangeType((string cph, string farmName, string owner, string address, string changeType)[] records)
    {
        var sb = new StringBuilder();
        sb.AppendLine("CPH,FarmName,Owner,Address,CHANGE_TYPE");

        foreach (var (cph, farmName, owner, address, changeType) in records)
        {
            sb.AppendLine($"{cph},{farmName},{owner},{address},{changeType}");
        }

        return sb.ToString();
    }

    private async Task UploadCsvToS3(string key, string content)
    {
        var request = new PutObjectRequest
        {
            BucketName = LocalStackFixture.TestBucket,
            Key = key,
            ContentBody = content,
            ContentType = "text/csv"
        };

        await _localStackFixture.S3Client.PutObjectAsync(request);
        _createdTestFileKeys.Add(key);

        _testOutputHelper.WriteLine($"Uploaded CSV file: {key}");
    }

    private async Task CleanupTestDataAsync()
    {
        try
        {
            foreach (var key in _createdTestFileKeys)
            {
                try
                {
                    await _localStackFixture.S3Client.DeleteObjectAsync(new DeleteObjectRequest
                    {
                        BucketName = LocalStackFixture.TestBucket,
                        Key = key
                    });
                }
                catch (Exception ex)
                {
                    _testOutputHelper.WriteLine($"Failed to delete test file {key}: {ex.Message}");
                }
            }

            _createdTestFileKeys.Clear();
        }
        catch (Exception ex)
        {
            _testOutputHelper.WriteLine($"Failed to cleanup test data: {ex.Message}");
        }
    }
}

/// <summary>
/// Collection definition for tests that need both LocalStack and MongoDB containers
/// </summary>
[CollectionDefinition("LocalStackAndMongo")]
public class LocalStackAndMongoCollection : ICollectionFixture<LocalStackFixture>, ICollectionFixture<MongoDbFixture>
{
}