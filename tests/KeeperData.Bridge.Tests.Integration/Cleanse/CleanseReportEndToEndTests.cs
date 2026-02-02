using System.Globalization;
using System.IO.Compression;
using Amazon.S3;
using Amazon.S3.Model;
using CsvHelper;
using CsvHelper.Configuration;
using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.Database;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Locking;
using KeeperData.Core.Querying.Impl;
using KeeperData.Core.Reports;
using KeeperData.Core.Reports.Abstract;
using KeeperData.Core.Reports.Domain;
using KeeperData.Core.Reports.Impl;
using KeeperData.Core.Reports.Strategies;
using KeeperData.Core.Storage;
using KeeperData.Infrastructure.Database.Configuration;
using KeeperData.Infrastructure.Locking;
using KeeperData.Infrastructure.Storage;
using MartinCostello.Logging.XUnit;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Cleanse;


/// <summary>
/// End-to-end integration tests for the complete cleanse report flow.
/// Uses TestContainers for MongoDB and LocalStack (S3).
/// Tests the happy path from analysis through CSV export to notification.
/// </summary>
public class CleanseReportEndToEndTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;

    // TestContainers
    private Testcontainers.MongoDb.MongoDbContainer _mongoContainer = null!;
    private Testcontainers.LocalStack.LocalStackContainer _localStackContainer = null!;

    // Clients
    private IMongoClient _mongoClient = null!;
    private IAmazonS3 _s3Client = null!;

    // System under test
    private CleanseReportService _cleanseReportService = null!;

    // Repositories
    private ICleanseReportRepository _reportRepository = null!;
    private ICleanseAnalysisRepository _analysisRepository = null!;

    // Fake for notification service
    private FakeNotificationService _fakeNotificationService = null!;

    // Configuration
    private const string TestDatabaseName = "cleanse-e2e-test";
    private const string TestBucket = "cleanse-reports-bucket";
    private const string CleanseReportsFolder = "cleanse-reports";

    // Collection names from StandardDataSetDefinitionsBuilder
    private const string CtsCphHoldingCollection = "cts_cph_holding";
    private const string SamCphHoldingCollection = "sam_cph_holdings";

    public CleanseReportEndToEndTests(ITestOutputHelper output)
    {
        _output = output;
    }

    #region Test Lifecycle

    public async Task InitializeAsync()
    {
        _output.WriteLine("=== Starting TestContainers ===");

        await StartMongoContainer();
        await StartLocalStackContainer();
        await CreateS3Bucket();
        await SetupServices();

        _output.WriteLine("=== TestContainers Ready ===");
    }

    public async Task DisposeAsync()
    {
        _output.WriteLine("=== Cleaning up TestContainers ===");

        try
        {
            await _mongoClient.DropDatabaseAsync(TestDatabaseName);
            _s3Client?.Dispose();
        }
        finally
        {
            await _mongoContainer.DisposeAsync();
            await _localStackContainer.DisposeAsync();
        }
    }

    private async Task StartMongoContainer()
    {
        _output.WriteLine("Starting MongoDB container...");

        _mongoContainer = new Testcontainers.MongoDb.MongoDbBuilder()
            .WithImage("mongo:7.0")
            .WithPortBinding(27017, true)
            .Build();

        await _mongoContainer.StartAsync();

        _mongoClient = new MongoClient(_mongoContainer.GetConnectionString());
        _output.WriteLine($"MongoDB ready at: {_mongoContainer.GetConnectionString()}");
    }

    private async Task StartLocalStackContainer()
    {
        _output.WriteLine("Starting LocalStack container...");

        _localStackContainer = new Testcontainers.LocalStack.LocalStackBuilder()
            .WithImage("localstack/localstack:2.3")
            .WithEnvironment("SERVICES", "s3")
            .WithEnvironment("AWS_DEFAULT_REGION", "us-east-1")
            .WithEnvironment("AWS_ACCESS_KEY_ID", "test")
            .WithEnvironment("AWS_SECRET_ACCESS_KEY", "test")
            .WithPortBinding(4566, true)
            .Build();

        await _localStackContainer.StartAsync();

        var config = new AmazonS3Config
        {
            ServiceURL = _localStackContainer.GetConnectionString(),
            ForcePathStyle = true,
            UseHttp = true,
            // Disable checksum validation for LocalStack compatibility
            RequestChecksumCalculation = Amazon.Runtime.RequestChecksumCalculation.WHEN_REQUIRED,
            ResponseChecksumValidation = Amazon.Runtime.ResponseChecksumValidation.WHEN_REQUIRED
        };

        _s3Client = new AmazonS3Client("test", "test", config);
        _output.WriteLine($"LocalStack ready at: {_localStackContainer.GetConnectionString()}");
    }

    private async Task CreateS3Bucket()
    {
        _output.WriteLine($"Creating S3 bucket: {TestBucket}");

        await _s3Client.PutBucketAsync(new PutBucketRequest
        {
            BucketName = TestBucket,
            UseClientRegion = true
        });
    }

    private Task SetupServices()
    {
        var mongoConfig = Options.Create<IDatabaseConfig>(new MongoConfig
        {
            DatabaseName = TestDatabaseName,
            DatabaseUri = _mongoContainer.GetConnectionString()
        });

        var dataSets = StandardDataSetDefinitionsBuilder.Build();

        // Create loggers that output to test output
        var loggerFactory = LoggerFactory.Create(builder => builder.AddXUnit(_output));

        var queryService = new QueryService(
            _mongoClient,
            mongoConfig,
            dataSets,
            loggerFactory.CreateLogger<QueryService>());

        _reportRepository = new CleanseReportRepository(_mongoClient, mongoConfig);
        _analysisRepository = new CleanseAnalysisRepository(_mongoClient, mongoConfig);

        var distributedLock = new MongoDistributedLock(
            Options.Create(new MongoConfig { DatabaseName = TestDatabaseName }),
            _mongoClient);

        // Create real S3 blob storage service pointing to LocalStack
        var blobStorageService = new S3BlobStorageService(
            _s3Client,
            loggerFactory.CreateLogger<S3BlobStorageService>(),
            TestBucket,
            CleanseReportsFolder);

        // Create a factory that returns our real S3 service
        var blobStorageServiceFactory = new TestBlobStorageServiceFactory(blobStorageService);

        // Create real export service using real S3
        var exportService = new CleanseReportExportService(
            _reportRepository,
            blobStorageServiceFactory,
            loggerFactory.CreateLogger<CleanseReportExportService>());

        // Create fake notification service to capture calls
        _fakeNotificationService = new FakeNotificationService();

        _cleanseReportService = new CleanseReportService(
            queryService,
            dataSets,
            _reportRepository,
            _analysisRepository,
            distributedLock,
            exportService,
            blobStorageServiceFactory,
            _fakeNotificationService,
            loggerFactory.CreateLogger<CleanseReportService>(),
            new ICleanseAnalysisStrategy[] { new CtsSamAnalysisStrategy() });

        return Task.CompletedTask;
    }

    #endregion

    #region End-to-End Tests

    [Fact, Trait("Dependence", "docker")]
    public async Task CleanseReport_WhenCtsCphNotInSam_ShouldDetectIssueExportToS3AndSendNotification()
    {
        // ========== ARRANGE ==========
        _output.WriteLine("=== ARRANGE: Setting up test data ===");

        // Create CTS holdings that do NOT have matching SAM records (will trigger issues)
        // Note: Region must be "AH" (Animal Health) as the analysis filters for LID_FULL_IDENTIFIER starting with "AH-"
        var missingCph1 = CreateLidFullIdentifier("AH", "12", "345", "0001");
        var missingCph2 = CreateLidFullIdentifier("AH", "12", "345", "0002");

        // Create a CTS holding that DOES have a matching SAM record (no issue)
        var matchingCph = CreateLidFullIdentifier("AH", "12", "345", "9999");

        await InsertCtsCphHolding(missingCph1);
        await InsertCtsCphHolding(missingCph2);
        await InsertCtsCphHolding(matchingCph);

        // Only insert SAM record for the matching one (with required supporting data)
        await InsertSamCphHoldingWithSupportingData(ExtractCph(matchingCph));

        LogArrangeComplete(missingCph1, missingCph2, matchingCph);

        // ========== ACT ==========
        _output.WriteLine("=== ACT: Running cleanse analysis ===");

        var operation = await RunAnalysisToCompletion();

        // ========== ASSERT ==========
        _output.WriteLine("=== ASSERT: Verifying results ===");

        AssertOperationCompleted(operation, expectedRecords: 3, expectedIssues: 2);
        AssertNotificationWasSent();

        var csvRecords = await DownloadAndExtractCsvFromS3(operation.ReportObjectKey!);
        AssertCsvContainsExpectedIssues(csvRecords, missingCph1, missingCph2);

        _output.WriteLine("=== TEST PASSED ===");
    }

    [Fact, Trait("Dependence", "docker")]
    public async Task CleanseReport_WhenNoIssuesFound_ShouldGenerateEmptyCsvAndNotify()
    {
        // ========== ARRANGE ==========
        _output.WriteLine("=== ARRANGE: Setting up test data with no issues ===");

        // Note: Region must be "AH" (Animal Health) as the analysis filters for LID_FULL_IDENTIFIER starting with "AH-"
        var cph = CreateLidFullIdentifier("AH", "12", "345", "8888");


        await InsertCtsCphHolding(cph);
        await InsertSamCphHoldingWithSupportingData(ExtractCph(cph));

        _output.WriteLine($"  - CTS holding: {cph}");
        _output.WriteLine($"  - SAM holding (with supporting data): {ExtractCph(cph)}");

        // ========== ACT ==========
        _output.WriteLine("=== ACT: Running cleanse analysis ===");

        var operation = await RunAnalysisToCompletion();

        // ========== ASSERT ==========
        _output.WriteLine("=== ASSERT: Verifying results ===");

        AssertOperationCompleted(operation, expectedRecords: 1, expectedIssues: 0);
        AssertNotificationWasSent();

        var csvRecords = await DownloadAndExtractCsvFromS3(operation.ReportObjectKey!);
        csvRecords.Should().BeEmpty("No issues should produce empty CSV");

        _output.WriteLine("CSV is empty as expected");
        _output.WriteLine("=== TEST PASSED ===");
    }

    #endregion

    #region Assertion Helpers

    private void AssertOperationCompleted(CleanseAnalysisOperation operation, int expectedRecords, int expectedIssues)
    {
        operation.Status.Should().Be(CleanseAnalysisStatus.Completed);
        operation.RecordsAnalyzed.Should().Be(expectedRecords);
        operation.IssuesFound.Should().Be(expectedIssues);
        operation.DurationMs.Should().BeGreaterThan(0);
        operation.ReportUrl.Should().NotBeNullOrEmpty("Report URL should be set");
        operation.ReportObjectKey.Should().NotBeNullOrEmpty("Object key should be set");

        _output.WriteLine($"Operation completed: {operation.RecordsAnalyzed} records, {operation.IssuesFound} issues");
        _output.WriteLine($"Report URL: {operation.ReportUrl}");
        _output.WriteLine($"Object key: {operation.ReportObjectKey}");
    }

    private void AssertNotificationWasSent()
    {
        _fakeNotificationService.WasCalled.Should().BeTrue("Notification should have been sent");
        _fakeNotificationService.LastReportUrl.Should().NotBeNullOrEmpty();
        _fakeNotificationService.LastReportUrl.Should().Contain(TestBucket, "URL should reference S3 bucket");

        _output.WriteLine($"Notification sent with URL: {_fakeNotificationService.LastReportUrl}");
    }

    private void AssertCsvContainsExpectedIssues(List<CsvIssueRecord> csvRecords, params string[] expectedLidFullIdentifiers)
    {
        csvRecords.Should().HaveCount(expectedLidFullIdentifiers.Length);

        foreach (var lidFullIdentifier in expectedLidFullIdentifiers)
        {
            var expectedCph = ExtractCph(lidFullIdentifier);
            csvRecords.Should().Contain(r => r.CPH == expectedCph,
                $"CSV should contain issue for CPH: {expectedCph}");
        }

        csvRecords.Should().AllSatisfy(r =>
            r.ErrorCode.Should().Be(IssueCodes.CTS_CPH_NOT_IN_SAM));

        _output.WriteLine($"CSV contains {csvRecords.Count} issues:");
        foreach (var record in csvRecords)
        {
            _output.WriteLine($"    - CPH: {record.CPH}, ErrorCode: {record.ErrorCode}");
        }
    }

    #endregion

    #region Data Setup Helpers

    /// <summary>
    /// Creates a LID_FULL_IDENTIFIER in format: {region}-{countyCode}/{parishCode}/{holdingCode}
    /// Example: EN-12/345/0001
    /// </summary>
    private static string CreateLidFullIdentifier(string region, string countyCode, string parishCode, string holdingCode)
        => $"{region}-{countyCode}/{parishCode}/{holdingCode}";

    /// <summary>
    /// Extracts CPH from LID_FULL_IDENTIFIER.
    /// Input:  EN-12/345/0001
    /// Output: 12/345/0001
    /// </summary>
    private static string ExtractCph(string lidFullIdentifier)
    {
        var hyphenIndex = lidFullIdentifier.IndexOf('-');
        return lidFullIdentifier[(hyphenIndex + 1)..];
    }

    private async Task InsertCtsCphHolding(string lidFullIdentifier)
    {
        var database = _mongoClient.GetDatabase(TestDatabaseName);
        var collection = database.GetCollection<BsonDocument>(CtsCphHoldingCollection);

        var document = new BsonDocument
        {
            { "_id", Guid.NewGuid().ToString() },
            { "LID_FULL_IDENTIFIER", lidFullIdentifier },
            { "LOC_EFFECTIVE_TO", BsonNull.Value },
            { "IsDeleted", false }
        };

        await collection.InsertOneAsync(document);
    }

    private async Task InsertSamCphHoldingWithSupportingData(string cph)
    {
        var database = _mongoClient.GetDatabase(TestDatabaseName);
        var partyId = $"PARTY_{Guid.NewGuid():N}"[..16];

        // Insert SAM CPH Holding
        var samCollection = database.GetCollection<BsonDocument>(SamCphHoldingCollection);
        await samCollection.InsertOneAsync(new BsonDocument
        {
            { "_id", Guid.NewGuid().ToString() },
            { "CPH", cph },
            { "IsDeleted", false }
        });

        // Insert SAM Herd (required for full rule chain)
        var herdCollection = database.GetCollection<BsonDocument>("sam_herd");
        await herdCollection.InsertOneAsync(new BsonDocument
        {
            { "_id", Guid.NewGuid().ToString() },
            { "CPHH", $"{cph}-001" },
            { "OWNER_PARTY_IDS", partyId },
            { "KEEPER_PARTY_IDS", BsonNull.Value },
            { "IsDeleted", false }
        });

        // Insert SAM Party with email (required for full rule chain)
        var partyCollection = database.GetCollection<BsonDocument>("sam_party");
        await partyCollection.InsertOneAsync(new BsonDocument
        {
            { "_id", Guid.NewGuid().ToString() },
            { "PARTY_ID", partyId },
            { "INTERNET_EMAIL_ADDRESS", "test@example.com" },
            { "IsDeleted", false }
        });
    }

    private void LogArrangeComplete(string missingCph1, string missingCph2, string matchingCph)
    {
        _output.WriteLine($"  - CTS holdings inserted:");
        _output.WriteLine($"    - {missingCph1} (NO SAM record - expect issue)");
        _output.WriteLine($"    - {missingCph2} (NO SAM record - expect issue)");
        _output.WriteLine($"    - {matchingCph} (HAS SAM record - no issue)");
        _output.WriteLine($"  - SAM holding only for: {ExtractCph(matchingCph)}");
    }

    #endregion

    #region Analysis Helpers

    private async Task<CleanseAnalysisOperation> RunAnalysisToCompletion()
    {
        // Use RunAnalysisAsync which runs synchronously on the caller thread,
        // allowing exceptions to propagate and ensuring completion before returning
        var operation = await _cleanseReportService.RunAnalysisAsync();
        operation.Should().NotBeNull("Analysis should complete successfully");

        _output.WriteLine($"  - Analysis completed: {operation!.Id} in {operation.DurationMs}ms");

        return operation;
    }

    #endregion

    #region S3/CSV Extraction Helpers

    private async Task<List<CsvIssueRecord>> DownloadAndExtractCsvFromS3(string objectKey)
    {
        _output.WriteLine($"  - Downloading zip from S3: {CleanseReportsFolder}/{objectKey}");

        var response = await _s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = TestBucket,
            Key = $"{CleanseReportsFolder}/{objectKey}"
        });

        using var memoryStream = new MemoryStream();
        await response.ResponseStream.CopyToAsync(memoryStream);
        var zipBytes = memoryStream.ToArray();

        _output.WriteLine($"  - Downloaded {zipBytes.Length} bytes");

        return ExtractCsvFromZip(zipBytes);
    }

    private List<CsvIssueRecord> ExtractCsvFromZip(byte[] zipBytes)
    {
        using var zipStream = new MemoryStream(zipBytes);
        using var archive = new ZipArchive(zipStream, ZipArchiveMode.Read);

        var csvEntry = archive.Entries.FirstOrDefault(e => e.Name.EndsWith(".csv"));
        csvEntry.Should().NotBeNull("Zip should contain a CSV file");

        _output.WriteLine($"  - Extracting CSV: {csvEntry!.Name}");

        using var csvStream = csvEntry.Open();
        using var reader = new StreamReader(csvStream);
        using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true
        });

        return csv.GetRecords<CsvIssueRecord>().ToList();
    }

    /// <summary>
    /// Record type for reading CSV data.
    /// </summary>
    private sealed class CsvIssueRecord
    {
        public string CPH { get; set; } = string.Empty;
        public string ErrorCode { get; set; } = string.Empty;
    }

    #endregion

    #region Test Doubles

    /// <summary>
    /// Fake notification service that captures calls for verification.
    /// </summary>
    private sealed class FakeNotificationService : ICleanseReportNotificationService
    {
        public bool WasCalled { get; private set; }
        public string? LastReportUrl { get; private set; }

        public Task<CleanseReportNotificationResult> SendReportNotificationAsync(
            string reportUrl,
            CancellationToken ct = default)
        {
            WasCalled = true;
            LastReportUrl = reportUrl;

            return Task.FromResult(new CleanseReportNotificationResult
            {
                Success = true,
                NotificationId = $"fake-notification-{Guid.NewGuid()}",
                Recipient = "test@example.com"
            });
        }

        public Task<CleanseReportNotificationResult> SendTestNotificationAsync(
            string testEmailAddress,
            CancellationToken ct = default)
        {
            return Task.FromResult(new CleanseReportNotificationResult
            {
                Success = true,
                NotificationId = $"fake-test-notification-{Guid.NewGuid()}",
                Recipient = testEmailAddress
            });
        }
    }

    /// <summary>
    /// Simple factory that returns the provided blob storage service.
    /// </summary>
    private sealed class TestBlobStorageServiceFactory : IBlobStorageServiceFactory
    {
        private readonly IBlobStorageService _service;

        public TestBlobStorageServiceFactory(IBlobStorageService service)
        {
            _service = service;
        }

        public IBlobStorageService Get() => _service;
        public IBlobStorageService GetCleanseReportsBlobService() => _service;
        public IBlobStorageServiceReadOnly GetSource(string type) => _service;
        public IBlobStorageServiceReadOnly GetSourceExternal() => _service;
        public IBlobStorageService GetSourceInternal() => _service;
    }

    #endregion
}
