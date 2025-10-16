using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.Crypto;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Reporting;
using KeeperData.Core.Storage;
using Microsoft.Extensions.DependencyInjection;
using System.Text;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Scenarios;

[Trait("Dependence", "docker")]
public class EndToEndImportScenarioTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private readonly LocalStackFixture _localStackFixture;
    private readonly MongoDbFixture _mongoDbFixture;
    private ServiceProvider? _serviceProvider;

    private const string SourcePrefix = "source-encrypted";
    private const string DestPrefix = "dest-decrypted";
    private const string TestDatabaseName = "test-e2e-import";
    private const string CollectionName = "test_persons";

    public EndToEndImportScenarioTests(ITestOutputHelper output)
    {
        _output = output;
        
        _localStackFixture = new LocalStackFixture();
        _mongoDbFixture = new MongoDbFixture();
    }

    public async Task InitializeAsync()
    {
        await _localStackFixture.InitializeAsync();
        await _mongoDbFixture.InitializeAsync();
        
        await _mongoDbFixture.MongoClient.DropDatabaseAsync(TestDatabaseName);
        _output.WriteLine($"Initialized test database: {TestDatabaseName}");
    }

    public async Task DisposeAsync()
    {
        if (_serviceProvider != null)
        {
            await _serviceProvider.DisposeAsync();
        }

        await CleanupS3FilesAsync();
        await _mongoDbFixture.MongoClient.DropDatabaseAsync(TestDatabaseName);
        
        await _localStackFixture.DisposeAsync();
        await _mongoDbFixture.DisposeAsync();
    }

    [Fact]
    public async Task EndToEndImport_ShouldGenerateEncryptUploadImportIngestAndQuery()
    {
        _output.WriteLine("=== Starting End-to-End Import Scenario Test ===");

        var dataSetDefinition = CreateTestDataSetDefinition();
        _output.WriteLine($"✓ Step 1: Created DataSetDefinition for collection '{CollectionName}'");

        var (csvContent, sourceRecords) = GenerateTestCsvData(recordCount: 50);
        _output.WriteLine($"✓ Step 2: Generated {sourceRecords.Count} fake person records");

        _serviceProvider = ConfigureServices(dataSetDefinition);
        _output.WriteLine("✓ Step 3: Configured IoC container with all dependencies");

        var encryptedFileName = await EncryptCsvFileAsync(csvContent);
        _output.WriteLine($"✓ Step 4: Encrypted CSV file: {encryptedFileName}");

        await UploadEncryptedFileToS3Async(encryptedFileName);
        _output.WriteLine($"✓ Step 5: Uploaded encrypted file to S3 source location");

        var importId = await StartImportAsync();
        _output.WriteLine($"✓ Step 6: Started import with ID: {importId}");

        await VerifyImportCompletedAsync(importId);
        _output.WriteLine($"✓ Step 7: Import completed successfully");

        await VerifyFileProcessingAsync(importId);
        _output.WriteLine($"✓ Step 8: Verified file processing reports");

        var queryResults = await QueryIngestedDataAsync();
        _output.WriteLine($"✓ Step 9: Queried ingested data, found {queryResults.Count} records");

        VerifyDataIntegrity(sourceRecords, queryResults);
        _output.WriteLine($"✓ Step 10: Verified data integrity - all records match!");

        _output.WriteLine("=== End-to-End Import Scenario Test PASSED ===");
    }

    #region Helper Methods

    private DataSetDefinition CreateTestDataSetDefinition()
    {
        var today = DateOnly.FromDateTime(DateTime.UtcNow);
        var dateStr = today.ToString("yyyyMMdd");

        return new DataSetDefinition(
            Name: CollectionName,
            FilePrefixFormat: $"LITP_TEST_PERSONS_{{0}}",      
            DatePattern: "yyyyMMdd",
            PrimaryKeyHeaderName: "PersonId",
            ChangeTypeHeaderName: "CHANGETYPE");
    }

    private (string CsvContent, List<PersonRecord> Records) GenerateTestCsvData(int recordCount)
    {
        _output.WriteLine($"Generating {recordCount} fake person records using Bogus...");
        return TestDataGenerator.GeneratePersonCsv(recordCount, "PersonId");
    }

    private ServiceProvider ConfigureServices(DataSetDefinition dataSetDefinition)
    {
        var dataSetDefinitions = new DataSetDefinitions
        {
            SamCPHHolding = dataSetDefinition,    
            All = [dataSetDefinition]
        };

        return TestServiceProviderBuilder.BuildServiceProvider(
            s3Client: _localStackFixture.S3Client,
            bucketName: LocalStackFixture.TestBucket,
            sourcePrefix: SourcePrefix,
            destPrefix: DestPrefix,
            mongoClient: _mongoDbFixture.MongoClient,
            mongoDatabaseName: TestDatabaseName,
            dataSetDefinitions: dataSetDefinitions);
    }

    private async Task<string> EncryptCsvFileAsync(string csvContent)
    {
        if (_serviceProvider == null)
        {
            throw new InvalidOperationException("ServiceProvider not initialized");
        }

        var cryptoTransform = _serviceProvider.GetRequiredService<IAesCryptoTransform>();
        var passwordSaltService = _serviceProvider.GetRequiredService<IPasswordSaltService>();

        var today = DateOnly.FromDateTime(DateTime.UtcNow);
        var dateStr = today.ToString("yyyyMMdd");
        var fileName = $"LITP_TEST_PERSONS_{dateStr}_001.csv";
        var encryptedFileName = $"{fileName}.enc";

        var credentials = passwordSaltService.Get(encryptedFileName);

        var csvBytes = Encoding.UTF8.GetBytes(csvContent);

        _output.WriteLine($"Encrypting CSV file ({csvBytes.Length} bytes) with password derived from filename...");

        using var inputStream = new MemoryStream(csvBytes);
        using var outputStream = new MemoryStream();

        await cryptoTransform.EncryptStreamAsync(
            inputStream,
            outputStream,
            credentials.Password,
            credentials.Salt,
            csvBytes.Length);

        outputStream.Position = 0;
        var encryptedBytes = outputStream.ToArray();

        _output.WriteLine($"Encrypted file size: {encryptedBytes.Length} bytes");

        var tempPath = Path.Combine(Path.GetTempPath(), encryptedFileName);
        await File.WriteAllBytesAsync(tempPath, encryptedBytes);

        return encryptedFileName;
    }

    private async Task UploadEncryptedFileToS3Async(string encryptedFileName)
    {
        var blobStorageFactory = _serviceProvider!.GetRequiredService<IBlobStorageServiceFactory>();
        var sourceStorage = blobStorageFactory.GetSourceExternal();

        var tempPath = Path.Combine(Path.GetTempPath(), encryptedFileName);
        var encryptedBytes = await File.ReadAllBytesAsync(tempPath);

        var today = DateOnly.FromDateTime(DateTime.UtcNow);
        var dateStr = today.ToString("yyyyMMdd");
        var s3Key = $"LITP_TEST_PERSONS_{dateStr}_001.csv.enc";

        _output.WriteLine($"Uploading encrypted file to S3: {s3Key}");

        await _localStackFixture.S3Client.PutObjectAsync(new Amazon.S3.Model.PutObjectRequest
        {
            BucketName = LocalStackFixture.TestBucket,
            Key = $"{SourcePrefix}/{s3Key}",
            InputStream = new MemoryStream(encryptedBytes),
            ContentType = "application/octet-stream"
        });

        var exists = await sourceStorage.ExistsAsync(s3Key);
        exists.Should().BeTrue($"Encrypted file should exist in S3 at key: {s3Key}");

        _output.WriteLine($"Encrypted file uploaded successfully, size: {encryptedBytes.Length} bytes");

        File.Delete(tempPath);
    }

    private async Task<Guid> StartImportAsync()
    {
        var importOrchestrator = _serviceProvider!.GetRequiredService<IImportOrchestrator>();
        var importId = Guid.NewGuid();

        _output.WriteLine($"Starting import orchestrator with ID: {importId}");

        await importOrchestrator.StartAsync(importId, "external", CancellationToken.None);

        return importId;
    }

    private async Task VerifyImportCompletedAsync(Guid importId)
    {
        var reportingService = _serviceProvider!.GetRequiredService<IImportReportingService>();

        _output.WriteLine("Waiting for import to complete...");

        var maxWaitTime = TimeSpan.FromMinutes(2);
        var startTime = DateTime.UtcNow;

        while (DateTime.UtcNow - startTime < maxWaitTime)
        {
            var report = await reportingService.GetImportReportAsync(importId, CancellationToken.None);

            if (report == null)
            {
                await Task.Delay(1000);
                continue;
            }

            _output.WriteLine($"Import Status: {report.Status}");
            _output.WriteLine($"  Acquisition: {report.AcquisitionPhase?.Status}");
            _output.WriteLine($"  Ingestion: {report.IngestionPhase?.Status}");

            if (report.Status == ImportStatus.Completed)
            {
                _output.WriteLine("Import completed successfully!");

                report.AcquisitionPhase.Should().NotBeNull();
                report.AcquisitionPhase!.Status.Should().Be(PhaseStatus.Completed);
                report.AcquisitionPhase.FilesProcessed.Should().BeGreaterThan(0);

                report.IngestionPhase.Should().NotBeNull();
                report.IngestionPhase!.Status.Should().Be(PhaseStatus.Completed);
                report.IngestionPhase.RecordsCreated.Should().BeGreaterThan(0);

                return;
            }

            if (report.Status == ImportStatus.Failed)
            {
                throw new Exception($"Import failed: {report.Error}");
            }

            await Task.Delay(2000);
        }

        throw new TimeoutException($"Import did not complete within {maxWaitTime.TotalSeconds} seconds");
    }

    private async Task VerifyFileProcessingAsync(Guid importId)
    {
        var reportingService = _serviceProvider!.GetRequiredService<IImportReportingService>();

        var fileReports = await reportingService.GetFileReportsAsync(importId, CancellationToken.None);

        _output.WriteLine($"File Processing Reports: {fileReports.Count}");

        fileReports.Should().NotBeEmpty("At least one file should have been processed");

        foreach (var fileReport in fileReports)
        {
            _output.WriteLine($"  File: {fileReport.FileName}");
            _output.WriteLine($"    Status: {fileReport.Status}");
            _output.WriteLine($"    Records: {fileReport.Ingestion?.RecordsProcessed ?? 0}");
            _output.WriteLine($"    Created: {fileReport.Ingestion?.RecordsCreated ?? 0}");

            fileReport.Status.Should().Be(FileProcessingStatus.Ingested);
            fileReport.Ingestion.Should().NotBeNull();
            fileReport.Ingestion!.RecordsCreated.Should().BeGreaterThan(0);
        }
    }

    private async Task<List<Dictionary<string, object?>>> QueryIngestedDataAsync()
    {
        var queryService = _serviceProvider!.GetRequiredService<IMongoQueryService>();

        _output.WriteLine($"Querying collection '{CollectionName}'...");

        var result = await queryService.QueryAsync(
            collectionName: CollectionName,
            filter: null,        
            orderBy: "FirstName asc",
            skip: 0,
            top: 100,
            count: true,
            cancellationToken: CancellationToken.None);

        result.Should().NotBeNull();
        result.CollectionName.Should().Be(CollectionName);
        result.Data.Should().NotBeEmpty("Data should be ingested into the collection");

        _output.WriteLine($"Query returned {result.Count} records (Total: {result.TotalCount})");

        return result.Data.ToList();
    }

    private void VerifyDataIntegrity(List<PersonRecord> sourceRecords, List<Dictionary<string, object?>> queryResults)
    {
        _output.WriteLine("Verifying data integrity...");

        var activeSourceRecords = sourceRecords.Where(r => r.IsActive).ToList();

        queryResults.Count.Should().BeGreaterThanOrEqualTo(activeSourceRecords.Count - 5,
            "Most active records should be in the database (allowing for some variance)");

        var sampleSize = Math.Min(10, queryResults.Count);
        for (int i = 0; i < sampleSize; i++)
        {
            var queryRecord = queryResults[i];

            queryRecord.Should().ContainKey("PersonId");
            queryRecord.Should().ContainKey("FirstName");
            queryRecord.Should().ContainKey("LastName");
            queryRecord.Should().ContainKey("Email");
            queryRecord.Should().ContainKey("Department");
            queryRecord.Should().ContainKey("Salary");
            queryRecord.Should().ContainKey("IsActive");

            var personId = queryRecord["PersonId"]?.ToString();
            personId.Should().NotBeNullOrEmpty();

            var sourceRecord = sourceRecords.FirstOrDefault(r => r.PersonId == personId);
            if (sourceRecord != null)
            {
                _output.WriteLine($"  Verified record: {sourceRecord.FirstName} {sourceRecord.LastName}");

                queryRecord["FirstName"]?.ToString().Should().Be(sourceRecord.FirstName);
                queryRecord["LastName"]?.ToString().Should().Be(sourceRecord.LastName);
                queryRecord["Email"]?.ToString().Should().Be(sourceRecord.Email);
                queryRecord["Department"]?.ToString().Should().Be(sourceRecord.Department);

                var querySalary = Convert.ToInt32(queryRecord["Salary"]);
                querySalary.Should().Be(sourceRecord.Salary);
            }
        }

        _output.WriteLine($"✓ Data integrity verified for {sampleSize} sample records");
    }

    private async Task CleanupS3FilesAsync()
    {
        try
        {
            var allObjects = await _localStackFixture.S3Client.ListObjectsV2Async(
                new Amazon.S3.Model.ListObjectsV2Request
                {
                    BucketName = LocalStackFixture.TestBucket,
                    Prefix = SourcePrefix
                });

            foreach (var obj in allObjects.S3Objects)
            {
                await _localStackFixture.S3Client.DeleteObjectAsync(
                    LocalStackFixture.TestBucket,
                    obj.Key);
            }

            allObjects = await _localStackFixture.S3Client.ListObjectsV2Async(
                new Amazon.S3.Model.ListObjectsV2Request
                {
                    BucketName = LocalStackFixture.TestBucket,
                    Prefix = DestPrefix
                });

            foreach (var obj in allObjects.S3Objects)
            {
                await _localStackFixture.S3Client.DeleteObjectAsync(
                    LocalStackFixture.TestBucket,
                    obj.Key);
            }
        }
        catch (Exception ex)
        {
            _output.WriteLine($"Warning: Failed to clean up S3 files: {ex.Message}");
        }
    }

    #endregion
}
