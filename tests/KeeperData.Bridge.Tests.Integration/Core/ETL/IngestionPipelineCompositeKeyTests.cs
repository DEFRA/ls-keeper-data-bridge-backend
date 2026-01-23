using Amazon.S3.Model;
using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.Database;
using KeeperData.Core.Database.Configuration;
using KeeperData.Core.Database.Resilience;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.ETL.Utils;
using KeeperData.Core.Reporting;
using KeeperData.Core.Reporting.Dtos;
using KeeperData.Core.Storage;
using KeeperData.Core.Telemetry;
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
using KeeperData.Core;

namespace KeeperData.Bridge.Tests.Integration.Core.ETL;

/// <summary>
/// Integration tests for IngestionPipeline composite key functionality.
/// Tests multi-column primary keys that are hashed using RecordIdGenerator for URL-safe IDs.
/// </summary>
[Collection("LocalStackAndMongo"), Trait("Dependence", "docker")]
public class IngestionPipelineCompositeKeyTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly LocalStackFixture _localStackFixture;
    private readonly MongoDbFixture _mongoDbFixture;
    private readonly List<string> _createdTestFileKeys = new();
    private readonly string _testDatabaseName = "composite-key-test-db";
    private readonly IngestionPipeline _ingestionPipeline;
    private readonly IMongoClient _mongoClient;
    private readonly Mock<ILogger<IngestionPipeline>> _loggerMock;
    private readonly RecordIdGenerator _recordIdGenerator = new();

    private const string TestTopLevelFolder = "litprd";
    private const string DestinationFolder = "dest";

    public IngestionPipelineCompositeKeyTests(ITestOutputHelper testOutputHelper,
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
        var csvRowCounterMock = new Mock<CsvRowCounter>(new Mock<ILogger<CsvRowCounter>>().Object);

        // Create mock for ResilientMongoOperations
        var resilientMongoOpsMock = new Mock<ResilientMongoOperations>(
            Options.Create(new MongoResilienceConfig()),
            new Mock<ILogger<ResilientMongoOperations>>().Object);

        var coreApplicationMetricsMock = new Mock<KeeperData.Core.Telemetry.IApplicationMetrics>();

        _ingestionPipeline = new IngestionPipeline(blobStorageFactory,
                                                   externalCatalogueServiceFactory,
                                                   _mongoClient,
                                                   mongoConfig,
                                                   reportingServiceMock.Object,
                                                   csvRowCounterMock.Object,
                                                   resilientMongoOpsMock.Object,
                                                   coreApplicationMetricsMock.Object,
                                                   _loggerMock.Object);
    }

    public async Task InitializeAsync()
    {
        await _mongoClient.DropDatabaseAsync(_testDatabaseName);
        _testOutputHelper.WriteLine($"Initialized test database: {_testDatabaseName}");
    }

    public async Task DisposeAsync()
    {
        await CleanupTestDataAsync();
        await _mongoClient.DropDatabaseAsync(_testDatabaseName);
    }

    [Fact]
    public async Task StartAsync_WithCompositeKey_TwoColumns_ShouldCreateHashedId()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);
        var dataSetDefinition = new DataSetDefinition(Name: "test_composite_key",
                                                      FilePrefixFormat: "TEST_COMPOSITE_{0}",
                                                      DatePattern: "yyyyMMdd",
                                                      PrimaryKeyHeaderNames: ["REGION", "FARM_ID"],
                                                      ChangeTypeHeaderName: "CHANGE_TYPE",
                                                      Accumulators: []);

        var csvContent = GenerateCompositeKeyCsv(
            [
                ("NORTH", "F001", "Farm Alpha", "I"),
                ("SOUTH", "F002", "Farm Beta", "I"),
                ("EAST", "F003", "Farm Gamma", "I")
            ]
         );

        var fileName = $"TEST_COMPOSITE_{testDate:yyyyMMdd}120000.csv";
        var fileKey = $"{DestinationFolder}/{fileName}";
        await UploadCsvToS3(fileKey, csvContent);

        var importId = Guid.NewGuid();

        // Act - Use custom definition
        await IngestWithCustomDefinition(importId, dataSetDefinition);

        // Assert
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("test_composite_key");

        var documents = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        documents.Should().HaveCount(3);

        // Verify composite _id format: URL-safe hash generated by RecordIdGenerator
        var doc1 = documents.FirstOrDefault(d => d["REGION"] == "NORTH" && d["FARM_ID"] == "F001");
        doc1.Should().NotBeNull();
        doc1!["_id"].AsString.Should().HaveLength(43, "SHA256 hash should be 43 characters");
        doc1["_id"].AsString.Should().MatchRegex(@"^[A-Za-z0-9\-_]+$", "Hash should be URL-safe");

        // Verify hash matches RecordIdGenerator output
        var expectedId = _recordIdGenerator.GenerateId("NORTH", "F001");
        doc1["_id"].AsString.Should().Be(expectedId);
        doc1["REGION"].Should().Be("NORTH");
        doc1["FARM_ID"].Should().Be("F001");
        doc1["NAME"].Should().Be("Farm Alpha");

        var doc2 = documents.FirstOrDefault(d => d["REGION"] == "SOUTH" && d["FARM_ID"] == "F002");
        doc2.Should().NotBeNull();
        var expectedId2 = _recordIdGenerator.GenerateId("SOUTH", "F002");
        doc2!["_id"].AsString.Should().Be(expectedId2);
        doc2["REGION"].Should().Be("SOUTH");
        doc2["FARM_ID"].Should().Be("F002");

        var doc3 = documents.FirstOrDefault(d => d["REGION"] == "EAST" && d["FARM_ID"] == "F003");
        doc3.Should().NotBeNull();
        var expectedId3 = _recordIdGenerator.GenerateId("EAST", "F003");
        doc3!["_id"].AsString.Should().Be(expectedId3);
        doc3["REGION"].Should().Be("EAST");
        doc3["FARM_ID"].Should().Be("F003");

        _testOutputHelper.WriteLine($"Successfully created composite keys as URL-safe hashes");
    }

    [Fact]
    public async Task StartAsync_WithCompositeKey_ThreeColumns_ShouldCreateHashedId()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);
        var dataSetDefinition = new DataSetDefinition(Name: "test_triple_key",
                                                      FilePrefixFormat: "TEST_TRIPLE_{0}",
                                                      DatePattern: "yyyyMMdd",
                                                      PrimaryKeyHeaderNames: ["COUNTRY", "REGION", "FARM_ID"],
                                                      ChangeTypeHeaderName: "CHANGE_TYPE",
                                                      Accumulators: []);

        var csvContent = GenerateTripleKeyCsv(
        [
            ("UK", "NORTH", "F001", "Farm Alpha", "I"),
            ("UK", "SOUTH", "F002", "Farm Beta", "I"),
            ("FR", "NORTH", "F001", "Farm Gamma", "I")
        ]);

        var fileName = $"TEST_TRIPLE_{testDate:yyyyMMdd}120000.csv";
        var fileKey = $"{DestinationFolder}/{fileName}";
        await UploadCsvToS3(fileKey, csvContent);

        var importId = Guid.NewGuid();

        // Act
        await IngestWithCustomDefinition(importId, dataSetDefinition);

        // Assert
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("test_triple_key");

        var documents = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        documents.Should().HaveCount(3);

        // Verify composite _id format: URL-safe hash
        var doc1 = documents.FirstOrDefault(d => d["COUNTRY"] == "UK" && d["REGION"] == "NORTH" && d["FARM_ID"] == "F001");
        doc1.Should().NotBeNull();
        var expectedId1 = _recordIdGenerator.GenerateId("UK", "NORTH", "F001");
        doc1!["_id"].AsString.Should().Be(expectedId1);
        doc1["COUNTRY"].Should().Be("UK");
        doc1["REGION"].Should().Be("NORTH");
        doc1["FARM_ID"].Should().Be("F001");
        doc1["NAME"].Should().Be("Farm Alpha");

        // Different COUNTRY with same REGION and FARM_ID should have different _id
        var doc3 = documents.FirstOrDefault(d => d["COUNTRY"] == "FR" && d["REGION"] == "NORTH" && d["FARM_ID"] == "F001");
        doc3.Should().NotBeNull();
        var expectedId3 = _recordIdGenerator.GenerateId("FR", "NORTH", "F001");
        doc3!["_id"].AsString.Should().Be(expectedId3);
        doc3["_id"].AsString.Should().NotBe(expectedId1, "Different composite keys should produce different hashes");
        doc3["COUNTRY"].Should().Be("FR");
        doc3["NAME"].Should().Be("Farm Gamma");

        _testOutputHelper.WriteLine($"Successfully created three-part composite keys as hashes");
    }

    [Fact]
    public async Task StartAsync_WithCompositeKey_ContainingSpecialCharacters_ShouldHandleCorrectly()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);
        var dataSetDefinition = new DataSetDefinition(Name: "test_special_chars",
                                                      FilePrefixFormat: "TEST_SPECIAL_{0}",
                                                      DatePattern: "yyyyMMdd",
                                                      PrimaryKeyHeaderNames: ["CPH", "HOLDER_ID"],
                                                      ChangeTypeHeaderName: "CHANGE_TYPE",
                                                      Accumulators: []);

        // CPH contains slashes, HOLDER_ID contains alphanumeric
        var csvContent = "CPH|HOLDER_ID|NAME|CHANGE_TYPE\n"
            + "12/345/6789|H001|Holder One|I\n"
            + "98/765/4321|H002|Holder Two|I\n"
            + "11/222/3333|H123ABC|Holder Three|I\n";

        var fileName = $"TEST_SPECIAL_{testDate:yyyyMMdd}120000.csv";
        var fileKey = $"{DestinationFolder}/{fileName}";
        await UploadCsvToS3(fileKey, csvContent);

        var importId = Guid.NewGuid();

        // Act
        await IngestWithCustomDefinition(importId, dataSetDefinition);

        // Assert
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("test_special_chars");

        var documents = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        documents.Should().HaveCount(3);

        // Verify composite _id with special characters is hashed correctly
        var doc1 = documents.FirstOrDefault(d => d["CPH"] == "12/345/6789" && d["HOLDER_ID"] == "H001");
        doc1.Should().NotBeNull();
        var expectedId1 = _recordIdGenerator.GenerateId("12/345/6789", "H001");
        doc1!["_id"].AsString.Should().Be(expectedId1);
        doc1["_id"].AsString.Should().MatchRegex(@"^[A-Za-z0-9\-_]+$", "Hash should be URL-safe despite special chars in input");
        doc1["CPH"].Should().Be("12/345/6789");
        doc1["HOLDER_ID"].Should().Be("H001");
        doc1["NAME"].Should().Be("Holder One");

        var doc3 = documents.FirstOrDefault(d => d["CPH"] == "11/222/3333" && d["HOLDER_ID"] == "H123ABC");
        doc3.Should().NotBeNull();
        var expectedId3 = _recordIdGenerator.GenerateId("11/222/3333", "H123ABC");
        doc3!["_id"].AsString.Should().Be(expectedId3);
        doc3["HOLDER_ID"].Should().Be("H123ABC");

        _testOutputHelper.WriteLine($"Successfully handled special characters in composite keys (hashed to URL-safe)");
    }

    [Fact]
    public async Task StartAsync_WithCompositeKey_DuplicateCompositeKey_ShouldUpsert()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);
        var dataSetDefinition = new DataSetDefinition(Name: "test_upsert",
                                                      FilePrefixFormat: "TEST_UPSERT_{0}",
                                                      DatePattern: "yyyyMMdd",
                                                      PrimaryKeyHeaderNames: ["REGION", "FARM_ID"],
                                                      ChangeTypeHeaderName: "CHANGE_TYPE",
                                                      Accumulators: []);

        // First import
        var csvContent1 = GenerateCompositeKeyCsv([("NORTH", "F001", "Farm Alpha", "I")]);
        var fileName1 = $"TEST_UPSERT_{testDate:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName1}", csvContent1);
        await IngestWithCustomDefinition(Guid.NewGuid(), dataSetDefinition);

        // Get original document
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("test_upsert");
        var expectedId = _recordIdGenerator.GenerateId("NORTH", "F001");
        var originalDoc = await collection.Find(d => d["_id"] == expectedId).FirstOrDefaultAsync();
        var originalCreatedAt = originalDoc["CreatedAtUtc"].ToUniversalTime();

        // Clean up first file
        await _localStackFixture.S3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = LocalStackFixture.TestBucket,
            Key = $"{DestinationFolder}/{fileName1}"
        });
        _createdTestFileKeys.Remove($"{DestinationFolder}/{fileName1}");

        await Task.Delay(100);

        // Second import with same composite key but different data
        var csvContent2 = GenerateCompositeKeyCsv([("NORTH", "F001", "Farm Alpha Updated", "U")]);
        var testDate2 = testDate.AddDays(-1);
        var fileName2 = $"TEST_UPSERT_{testDate2:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName2}", csvContent2);

        // Act
        await IngestWithCustomDefinition(Guid.NewGuid(), dataSetDefinition);

        // Assert
        var documents = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        documents.Should().HaveCount(1, "Should have upserted the existing document");

        var updatedDoc = documents[0];
        updatedDoc["_id"].Should().Be(expectedId);
        updatedDoc["NAME"].Should().Be("Farm Alpha Updated");
        updatedDoc["REGION"].Should().Be("NORTH");
        updatedDoc["FARM_ID"].Should().Be("F001");

        // Verify CreatedAtUtc was preserved
        updatedDoc["CreatedAtUtc"].ToUniversalTime().Should().Be(originalCreatedAt);

        // Verify UpdatedAtUtc was updated
        updatedDoc["UpdatedAtUtc"].ToUniversalTime().Should().BeAfter(originalCreatedAt);

        _testOutputHelper.WriteLine("Successfully upserted document with composite key (same hash)");
    }

    [Fact]
    public async Task StartAsync_WithCompositeKey_SoftDelete_ShouldMarkAsDeleted()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);
        var dataSetDefinition = new DataSetDefinition(Name: "test_delete",
                                                      FilePrefixFormat: "TEST_DELETE_{0}",
                                                      DatePattern: "yyyyMMdd",
                                                      PrimaryKeyHeaderNames: ["REGION", "FARM_ID"],
                                                      ChangeTypeHeaderName: "CHANGE_TYPE",
                                                      Accumulators: []);

        // First import - create record
        var csvContent1 = GenerateCompositeKeyCsv(new[]
        {
            ("WEST", "F999", "Farm to Delete", "I")
        });

        var fileName1 = $"TEST_DELETE_{testDate:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName1}", csvContent1);
        await IngestWithCustomDefinition(Guid.NewGuid(), dataSetDefinition);

        // Clean up first file
        await _localStackFixture.S3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = LocalStackFixture.TestBucket,
            Key = $"{DestinationFolder}/{fileName1}"
        });
        _createdTestFileKeys.Remove($"{DestinationFolder}/{fileName1}");

        // Second import - soft delete
        var csvContent2 = GenerateCompositeKeyCsv(new[]
        {
            ("WEST", "F999", "Farm to Delete", "D")
        });

        var testDate2 = testDate.AddDays(-1);
        var fileName2 = $"TEST_DELETE_{testDate2:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName2}", csvContent2);

        // Act
        await IngestWithCustomDefinition(Guid.NewGuid(), dataSetDefinition);

        // Assert
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("test_delete");
        var expectedId = _recordIdGenerator.GenerateId("WEST", "F999");
        var doc = await collection.Find(d => d["_id"] == expectedId).FirstOrDefaultAsync();

        doc.Should().NotBeNull();
        doc!["IsDeleted"].AsBoolean.Should().BeTrue();
        doc.Contains("DeletedAtUtc").Should().BeTrue();
        doc["REGION"].Should().Be("WEST");
        doc["FARM_ID"].Should().Be("F999");

        _testOutputHelper.WriteLine("Successfully soft-deleted record with composite key (hash-based)");
    }

    [Fact]
    public async Task StartAsync_WithCompositeKey_MissingOneKeyColumn_ShouldThrowException()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);
        var dataSetDefinition = new DataSetDefinition(Name: "test_missing_key",
                                                      FilePrefixFormat: "TEST_MISSING_{0}",
                                                      DatePattern: "yyyyMMdd",
                                                      PrimaryKeyHeaderNames: ["REGION", "FARM_ID"],
                                                      ChangeTypeHeaderName: "CHANGE_TYPE",
                                                      Accumulators: []);

        // CSV missing FARM_ID column
        var csvContent = "REGION|NAME|CHANGE_TYPE\nNORTH|Farm Alpha|I\n";

        var fileName = $"TEST_MISSING_{testDate:yyyyMMdd}120000.csv";
        var fileKey = $"{DestinationFolder}/{fileName}";
        await UploadCsvToS3(fileKey, csvContent);

        var importId = Guid.NewGuid();

        // Act
        var act = async () => await IngestWithCustomDefinition(importId, dataSetDefinition);

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Primary key header(s) 'FARM_ID' not found*");

        _testOutputHelper.WriteLine("Successfully validated missing composite key column");
    }

    [Fact]
    public async Task StartAsync_WithCompositeKey_NullKeyPart_ShouldCreateIdWithEmptyString()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);
        var dataSetDefinition = new DataSetDefinition(Name: "test_null_key",
                                                      FilePrefixFormat: "TEST_NULL_{0}",
                                                      DatePattern: "yyyyMMdd",
                                                      PrimaryKeyHeaderNames: ["REGION", "FARM_ID"],
                                                      ChangeTypeHeaderName: "CHANGE_TYPE",
                                                      Accumulators: []);

        // CSV with null/empty value in one key part
        var csvContent = "REGION|FARM_ID|NAME|CHANGE_TYPE\n"
            + "NORTH|F001|Farm Alpha|I\n"
            + "|F002|Farm Beta|I\n"
            + "SOUTH||Farm Gamma|I\n";

        var fileName = $"TEST_NULL_{testDate:yyyyMMdd}120000.csv";
        var fileKey = $"{DestinationFolder}/{fileName}";
        await UploadCsvToS3(fileKey, csvContent);

        var importId = Guid.NewGuid();

        // Act & Assert - this should throw because RecordIdGenerator doesn't allow empty key parts
        var act = async () => await IngestWithCustomDefinition(importId, dataSetDefinition);

        await act.Should().ThrowAsync<ArgumentException>()
            .WithMessage("*null or empty*");

        _testOutputHelper.WriteLine("Successfully validated that null/empty key parts are rejected");
    }

    [Fact]
    public async Task StartAsync_WithCompositeKey_AndAccumulators_ShouldWorkTogether()
    {
        // Arrange
        var testDate = new DateOnly(2024, 12, 15);
        var dataSetDefinition = new DataSetDefinition(Name: "test_composite_accum",
                                                      FilePrefixFormat: "TEST_COMP_ACC_{0}",
                                                      DatePattern: "yyyyMMdd",
                                                      PrimaryKeyHeaderNames: ["REGION", "FARM_ID"],
                                                      ChangeTypeHeaderName: "CHANGE_TYPE",
                                                      Accumulators: ["DISEASE_TYPE", "ANIMAL_CODE"]);

        // First import
        var csvContent1 = "REGION|FARM_ID|NAME|DISEASE_TYPE|ANIMAL_CODE|CHANGE_TYPE\n"
             + "NORTH|F001|Farm Alpha|BVD|BOVINE|I\n";
        var fileName1 = $"TEST_COMP_ACC_{testDate:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName1}", csvContent1);
        await IngestWithCustomDefinition(Guid.NewGuid(), dataSetDefinition);

        // Clean up first file
        await _localStackFixture.S3Client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = LocalStackFixture.TestBucket,
            Key = $"{DestinationFolder}/{fileName1}"
        });
        _createdTestFileKeys.Remove($"{DestinationFolder}/{fileName1}");

        // Second import - same composite key, different accumulator values
        var csvContent2 = "REGION|FARM_ID|NAME|DISEASE_TYPE|ANIMAL_CODE|CHANGE_TYPE\n" +
            "NORTH|F001|Farm Alpha Updated|IBR|OVINE|U\n";
        var testDate2 = testDate.AddDays(-1);
        var fileName2 = $"TEST_COMP_ACC_{testDate2:yyyyMMdd}120000.csv";
        await UploadCsvToS3($"{DestinationFolder}/{fileName2}", csvContent2);

        // Act
        await IngestWithCustomDefinition(Guid.NewGuid(), dataSetDefinition);

        // Assert
        var database = _mongoClient.GetDatabase(_testDatabaseName);
        var collection = database.GetCollection<BsonDocument>("test_composite_accum");
        var expectedId = _recordIdGenerator.GenerateId("NORTH", "F001");
        var doc = await collection.Find(d => d["_id"] == expectedId).FirstOrDefaultAsync();

        doc.Should().NotBeNull();

        // Verify composite key
        doc!["REGION"].Should().Be("NORTH");
        doc["FARM_ID"].Should().Be("F001");

        // Verify non-accumulator field was overwritten
        doc["NAME"].AsString.Should().Be("Farm Alpha Updated");

        // Verify accumulator fields accumulated distinct values
        doc["DISEASE_TYPE"].AsBsonArray.Should().HaveCount(2);
        doc["DISEASE_TYPE"].AsBsonArray.Select(v => v.AsString).Should().BeEquivalentTo(new[] { "BVD", "IBR" });

        doc["ANIMAL_CODE"].AsBsonArray.Should().HaveCount(2);
        doc["ANIMAL_CODE"].AsBsonArray.Select(v => v.AsString).Should().BeEquivalentTo(new[] { "BOVINE", "OVINE" });

        _testOutputHelper.WriteLine("Successfully combined composite keys (hashed) with accumulators");
    }

    private async Task IngestWithCustomDefinition(Guid importId, DataSetDefinition dataSetDefinition)
    {
        // Create a custom catalogue service with the test definition
        var blobStorageFactory = CreateBlobStorageFactory();
        var loggerMock = new Mock<ILogger<S3BlobStorageServiceReadOnly>>();
        var destBlobService = new S3BlobStorageServiceReadOnly(_localStackFixture.S3Client,
                                                               loggerMock.Object,
                                                               LocalStackFixture.TestBucket,
                                                               DestinationFolder);

        var timeProvider = new FakeTimeProvider(new DateTimeOffset(2024, 12, 15, 10, 0, 0, TimeSpan.Zero));
        var definitions = new DataSetDefinitions
        {
            SamCPHHolding = dataSetDefinition,
            CTSCPHHolding = dataSetDefinition,
            CTSKeeper = dataSetDefinition,
            SamCPHHolder = dataSetDefinition,
            SamHerd = dataSetDefinition,
            SamParty = dataSetDefinition,
            All = [dataSetDefinition]
        };

        var catalogueService = new ExternalCatalogueService(destBlobService, timeProvider, definitions);
        var catalogueFactory = new Mock<IExternalCatalogueServiceFactory>();
        catalogueFactory.Setup(x => x.Create(It.IsAny<IBlobStorageServiceReadOnly>())).Returns(catalogueService);

        var mongoConfig = Options.Create<IDatabaseConfig>(new MongoConfig
        {
            DatabaseName = _testDatabaseName,
            DatabaseUri = _mongoDbFixture.ConnectionString,
            EnableTransactions = false,
            HealthcheckEnabled = false
        });

        var reportingServiceMock = new Mock<IImportReportingService>();

        // Create mock for ResilientMongoOperations
        var resilientMongoOpsMock = new Mock<ResilientMongoOperations>(
            Options.Create(new MongoResilienceConfig()),
            new Mock<ILogger<ResilientMongoOperations>>().Object);

        var coreApplicationMetricsMock = new Mock<KeeperData.Core.Telemetry.IApplicationMetrics>();

        var pipeline = new IngestionPipeline(blobStorageFactory,
                                             catalogueFactory.Object,
                                             _mongoClient,
                                             mongoConfig,
                                             reportingServiceMock.Object,
                                             new Mock<CsvRowCounter>(new Mock<ILogger<CsvRowCounter>>().Object).Object,
                                             resilientMongoOpsMock.Object,
                                             coreApplicationMetricsMock.Object,
                                             _loggerMock.Object);

        var report = new ImportReport
        {
            ImportId = importId,
            SourceType = "External",
            Status = ImportStatus.Started,
            StartedAtUtc = DateTime.UtcNow,
            AcquisitionPhase = new AcquisitionPhaseReport
            {
                Status = PhaseStatus.NotStarted,
                FilesDiscovered = 0,
                FilesProcessed = 0,
                FilesFailed = 0
            },
            IngestionPhase = new IngestionPhaseReport
            {
                Status = PhaseStatus.NotStarted,
                FilesProcessed = 0,
                RecordsCreated = 0,
                RecordsUpdated = 0,
                RecordsDeleted = 0
            }
        };

        await pipeline.StartAsync(report, CancellationToken.None);
    }

    private IBlobStorageServiceFactory CreateBlobStorageFactory()
    {
        var loggerFactory = new Mock<ILoggerFactory>();
        loggerFactory.Setup(x => x.CreateLogger(It.IsAny<string>())).Returns(new Mock<ILogger>().Object);

        var s3ClientFactory = new Mock<IS3ClientFactory>();
        s3ClientFactory.Setup(x => x.GetClientInfo<InternalStorageClient>())
            .Returns(new S3ClientFactory.ClientInfo(_localStackFixture.S3Client, LocalStackFixture.TestBucket));

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

    private string GenerateCompositeKeyCsv((string key1, string key2, string name, string changeType)[] records)
    {
        var sb = new StringBuilder();
        sb.AppendLine("REGION|FARM_ID|NAME|CHANGE_TYPE");

        foreach (var (key1, key2, name, changeType) in records)
        {
            sb.AppendLine($"{key1}|{key2}|{name}|{changeType}");
        }

        return sb.ToString();
    }

    private string GenerateTripleKeyCsv((string key1, string key2, string key3, string name, string changeType)[] records)
    {
        var sb = new StringBuilder();
        sb.AppendLine("COUNTRY|REGION|FARM_ID|NAME|CHANGE_TYPE");

        foreach (var (key1, key2, key3, name, changeType) in records)
        {
            sb.AppendLine($"{key1}|{key2}|{key3}|{name}|{changeType}");
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
}