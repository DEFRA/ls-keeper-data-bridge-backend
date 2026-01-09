using FluentAssertions;
using KeeperData.Core.Crypto;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.ETL.Utils;
using KeeperData.Core.Querying.Abstract;
using KeeperData.Core.Reporting;
using KeeperData.Core.Reporting.Services;
using KeeperData.Core.Storage;
using KeeperData.Infrastructure.Crypto;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Factories;
using KeeperData.Infrastructure.Storage.Factories.Implementations;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Text;
using Testcontainers.LocalStack;
using Testcontainers.MongoDb;
using Xunit.Abstractions;
using Amazon.S3;
using Amazon.S3.Model;
using MongoDB.Driver;
using Microsoft.Extensions.Options;
using KeeperData.Infrastructure.Database.Configuration;
using KeeperData.Core.Database;
using KeeperData.Core.Reporting.Impl;
using KeeperData.Core.Querying.Impl;
using KeeperData.Core;

namespace KeeperData.Bridge.PerformanceTests;

[Trait("testtype", "performance")]
public class PerformanceTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private LocalStackContainer? _localStackContainer;
    private MongoDbContainer? _mongoDbContainer;
    private IAmazonS3? _s3Client;
    private IMongoClient? _mongoClient;
    private ServiceProvider? _serviceProvider;

    private const string SourcePrefix = "perf-test-source";
    private const string DestPrefix = "perf-test-dest";
    private const string TestDatabaseName = "perf-test-db";
    private const string TestBucket = "perf-test-bucket";

    public PerformanceTests(ITestOutputHelper output)
    {
        _output = output;
    }

    public async Task InitializeAsync()
    {
        _output.WriteLine("=== Initializing Performance Test Environment ===");

        // Start LocalStack container
        _output.WriteLine("Starting LocalStack container...");
        _localStackContainer = new LocalStackBuilder()
            .WithImage("localstack/localstack:3.0")
            .Build();

        await _localStackContainer.StartAsync();
        _output.WriteLine(" LocalStack container started");

        // Create S3 client
        _s3Client = new AmazonS3Client(
            new Amazon.Runtime.BasicAWSCredentials("test", "test"),
            new AmazonS3Config
            {
                ServiceURL = _localStackContainer.GetConnectionString(),
                ForcePathStyle = true
            });

        // Create S3 bucket
        await _s3Client.PutBucketAsync(TestBucket);
        _output.WriteLine($" Created S3 bucket: {TestBucket}");

        // Start MongoDB container
        _output.WriteLine("Starting MongoDB container...");
        _mongoDbContainer = new MongoDbBuilder()
            .WithImage("mongo:7.0")
            .Build();

        await _mongoDbContainer.StartAsync();
        _output.WriteLine(" MongoDB container started");

        // Create MongoDB client
        _mongoClient = new MongoClient(_mongoDbContainer.GetConnectionString());
        await _mongoClient.DropDatabaseAsync(TestDatabaseName);
        _output.WriteLine($" Initialized MongoDB database: {TestDatabaseName}");

        _output.WriteLine("=== Environment Ready ===\n");
    }

    public async Task DisposeAsync()
    {
        _output.WriteLine("\n=== Cleaning Up Performance Test Environment ===");

        if (_serviceProvider != null)
        {
            await _serviceProvider.DisposeAsync();
            _output.WriteLine(" Disposed service provider");
        }

        if (_mongoClient != null)
        {
            await _mongoClient.DropDatabaseAsync(TestDatabaseName);
            _output.WriteLine(" Dropped MongoDB database");
        }

        if (_localStackContainer != null)
        {
            await _localStackContainer.DisposeAsync();
            _output.WriteLine(" Disposed LocalStack container");
        }

        if (_mongoDbContainer != null)
        {
            await _mongoDbContainer.DisposeAsync();
            _output.WriteLine(" Disposed MongoDB container");
        }

        _output.WriteLine("=== Cleanup Complete ===");
    }

    [Fact]
    public async Task FullPipeline_SamCPHHolding_NRecords_ShouldCompleteSuccessfully()
    {
        const int TotalToProcess = 100_000;

        _output.WriteLine("╔══════════════════════════════════════════════════════════════╗");
        _output.WriteLine("║  PERFORMANCE TEST: SAM CPH Holding - 1000 Records Pipeline  ║");
        _output.WriteLine("╚══════════════════════════════════════════════════════════════╝\n");

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Step 1: Generate or load cached test data
        var (csvContent, recordCount) = await GetOrGenerateTestDataAsync(TotalToProcess);
        _output.WriteLine($" Step 1: Test data ready ({recordCount} records) - {stopwatch.ElapsedMilliseconds}ms\n");

        // Step 2: Create dataset definition
        var dataSetDefinition = CreateSamCPHHoldingDefinition();
        _output.WriteLine($" Step 2: Created dataset definition for '{dataSetDefinition.Name}' - {stopwatch.ElapsedMilliseconds}ms\n");

        // Step 3: Configure services
        _serviceProvider = ConfigureServices(dataSetDefinition);
        _output.WriteLine($" Step 3: Configured service provider - {stopwatch.ElapsedMilliseconds}ms\n");

        // Step 4: Encrypt CSV file
        var encryptedFileName = await EncryptCsvFileAsync(csvContent);
        _output.WriteLine($" Step 4: Encrypted CSV file: {encryptedFileName} - {stopwatch.ElapsedMilliseconds}ms\n");

        // Step 5: Upload to S3
        await UploadEncryptedFileToS3Async(encryptedFileName);
        _output.WriteLine($" Step 5: Uploaded encrypted file to S3 - {stopwatch.ElapsedMilliseconds}ms\n");

        // Step 6: Start import
        var importId = await StartImportAsync();
        _output.WriteLine($" Step 6: Started import (ID: {importId}) - {stopwatch.ElapsedMilliseconds}ms\n");

        // Step 7: Wait for import completion
        await VerifyImportCompletedAsync(importId);
        _output.WriteLine($" Step 7: Import completed successfully - {stopwatch.ElapsedMilliseconds}ms\n");

        // Step 8: Verify file processing
        await VerifyFileProcessingAsync(importId);
        _output.WriteLine($" Step 8: File processing verified - {stopwatch.ElapsedMilliseconds}ms\n");

        // Step 9: Query ingested data
        var queryResults = await QueryIngestedDataAsync(dataSetDefinition.Name);
        _output.WriteLine($" Step 9: Queried ingested data ({queryResults.Count} records) - {stopwatch.ElapsedMilliseconds}ms\n");

        // Step 10: Verify data integrity
        VerifyDataIntegrity(queryResults, recordCount);

        stopwatch.Stop();

        _output.WriteLine($" Step 10: Data integrity verified - {stopwatch.ElapsedMilliseconds}ms\n");

        _output.WriteLine("╔══════════════════════════════════════════════════════════════╗");
        _output.WriteLine($"║  PERFORMANCE TEST COMPLETED                                  ║");
        _output.WriteLine($"║  Total Duration: {stopwatch.ElapsedMilliseconds}ms ({stopwatch.Elapsed.TotalSeconds:F2}s)                    ║");
        _output.WriteLine($"║  Records Processed: {recordCount}                                     ║");
        _output.WriteLine($"║  Throughput: {recordCount / stopwatch.Elapsed.TotalSeconds:F2} records/sec                        ║");
        _output.WriteLine("╚══════════════════════════════════════════════════════════════╝");
    }

    #region Helper Methods

    private async Task<(string CsvContent, int RecordCount)> GetOrGenerateTestDataAsync(int totalToProcess)
    {
        var cachedDataFileName = $"cached_sam_cph_holding_{totalToProcess}.csv";

        var cacheDir = Path.Combine(Path.GetTempPath(), "KeeperDataPerformanceTests");
        Directory.CreateDirectory(cacheDir);

        var cachedFilePath = Path.Combine(cacheDir, cachedDataFileName);

        if (File.Exists(cachedFilePath))
        {
            _output.WriteLine($"Loading cached test data from: {cachedFilePath}");
            var content = await File.ReadAllTextAsync(cachedFilePath);
            var lineCount = content.Split('\n', StringSplitOptions.RemoveEmptyEntries).Length - 1; // Exclude header
            _output.WriteLine($"Loaded {lineCount} records from cache");
            return (content, lineCount);
        }

        _output.WriteLine($"Generating new test data ({totalToProcess} records)...");
        var (csvContent, recordCount) = GenerateSamCPHHoldingData(totalToProcess);

        await File.WriteAllTextAsync(cachedFilePath, csvContent);
        _output.WriteLine($"Cached test data to: {cachedFilePath}");

        return (csvContent, recordCount);
    }

    private (string CsvContent, int RecordCount) GenerateSamCPHHoldingData(int recordCount)
    {
        var csv = new StringBuilder();

        // Write header based on samCPHHolding definition
        csv.AppendLine("CPH|ADDRESS_PK|DISEASE_TYPE|INTERVAL|INTERVAL_UNIT_OF_TIME|CPH_RELATIONSHIP_TYPE|SECONDARY_CPH|ANIMAL_SPECIES_CODE|ANIMAL_PRODUCTION_USAGE_CODE|CHANGETYPE");

        var random = new Random(42); // Fixed seed for reproducibility
        var diseaseTypes = new[] { "TB", "BRUCELLOSIS", "BSE", "SCRAPIE", "BLUETONGUE" };
        var intervalUnits = new[] { "DAYS", "WEEKS", "MONTHS", "YEARS" };
        var relationshipTypes = new[] { "PRIMARY", "SECONDARY", "TEMPORARY" };
        var speciesCodes = new[] { "CATTLE", "SHEEP", "GOAT", "PIG", "POULTRY" };
        var usageCodes = new[] { "BREEDING", "FATTENING", "DAIRY", "MIXED" };

        for (int i = 1; i <= recordCount; i++)
        {
            var cph = $"{random.Next(10, 100)}/{GenerateRandomCode(random, 7)}";
            var addressPk = random.Next(100000, 999999);
            var diseaseType = diseaseTypes[random.Next(diseaseTypes.Length)];
            var interval = random.Next(1, 365);
            var intervalUnit = intervalUnits[random.Next(intervalUnits.Length)];
            var relationshipType = relationshipTypes[random.Next(relationshipTypes.Length)];
            var secondaryCph = random.Next(2) == 0 ? $"{random.Next(10, 100)}/{GenerateRandomCode(random, 7)}" : "";
            var speciesCode = speciesCodes[random.Next(speciesCodes.Length)];
            var usageCode = usageCodes[random.Next(usageCodes.Length)];
            var changeType = "I"; // All inserts for initial load

            csv.AppendLine($"{cph}|{addressPk}|{diseaseType}|{interval}|{intervalUnit}|{relationshipType}|{secondaryCph}|{speciesCode}|{usageCode}|{changeType}");
        }

        return (csv.ToString(), recordCount);
    }

    private string GenerateRandomCode(Random random, int length)
    {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        return new string(Enumerable.Range(0, length)
            .Select(_ => chars[random.Next(chars.Length)])
            .ToArray());
    }

    private DataSetDefinition CreateSamCPHHoldingDefinition()
    {
        var today = DateOnly.FromDateTime(DateTime.UtcNow);

        return new DataSetDefinition(
            Name: "sam_cph_holdings",
            FilePrefixFormat: "LITP_SAMCPHHOLDING_{0}",
            DatePattern: "yyyyMMdd",
            PrimaryKeyHeaderNames: ["CPH"],
            ChangeTypeHeaderName: "CHANGETYPE",
            Accumulators: [
                "ADDRESS_PK",
                "DISEASE_TYPE",
                "INTERVAL",
                "INTERVAL_UNIT_OF_TIME",
                "CPH_RELATIONSHIP_TYPE",
                "SECONDARY_CPH",
                "ANIMAL_SPECIES_CODE",
                "ANIMAL_PRODUCTION_USAGE_CODE"
            ]);
    }

    private ServiceProvider ConfigureServices(DataSetDefinition dataSetDefinition)
    {
        var services = new ServiceCollection();

        services.AddLogging(builder =>
        {
            builder.AddConsole();
            builder.SetMinimumLevel(LogLevel.Warning); // Reduce noise in performance tests
        });

        services.AddSingleton(TimeProvider.System);

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["AesSalt"] = "Jr8Lm2PXzd7qNbVyWutRfGBxhkHTpE"
            }!)
            .Build();
        services.AddSingleton<IConfiguration>(configuration);

        // MongoDB configuration
        var mongoConfig = new MongoConfig
        {
            DatabaseName = TestDatabaseName,
            DatabaseUri = _mongoDbContainer!.GetConnectionString(),
            EnableTransactions = false,
            HealthcheckEnabled = false
        };
        services.AddSingleton<IOptions<MongoConfig>>(Options.Create(mongoConfig));
        services.AddSingleton<IOptions<IDatabaseConfig>>(Options.Create<IDatabaseConfig>(mongoConfig));
        services.AddSingleton(_mongoClient!);

        var resilenceSection = configuration.GetSection("MongoResilience");
        services.Configure<KeeperData.Core.Database.Configuration.MongoResilienceConfig>(resilenceSection);

        services.AddSingleton<KeeperData.Core.Database.Resilience.ResilientMongoOperations>();

        var dataSetDefinitions = new DataSetDefinitions
        {
            SamCPHHolding = dataSetDefinition,
            CTSCPHHolding = dataSetDefinition,
            CTSKeeper = dataSetDefinition,
            SamCPHHolder = dataSetDefinition,
            SamHerd = dataSetDefinition,
            SamParty = dataSetDefinition,
            All = [dataSetDefinition]
        };
        services.AddSingleton<Core.ETL.Abstract.IDataSetDefinitions>(dataSetDefinitions);

        var storageConfig = new KeeperData.Infrastructure.Storage.Configuration.StorageConfiguration
        {
            ExternalStorage = new KeeperData.Infrastructure.Storage.Configuration.StorageWithCredentialsConfiguration
            {
                BucketName = TestBucket,
                AccessKeySecretName = "not-used",
                SecretKeySecretName = "not-used",
                HealthcheckEnabled = false
            },
            InternalStorage = new KeeperData.Infrastructure.Storage.Configuration.StorageConfigurationDetails
            {
                BucketName = TestBucket,
                HealthcheckEnabled = false
            },
            SourceExternalPrefix = SourcePrefix,
            SourceInternalPrefix = DestPrefix,
            TargetInternalPrefix = DestPrefix
        };
        services.AddSingleton(storageConfig);

        // S3 client factory
        var s3Config = new AmazonS3Config
        {
            ServiceURL = _localStackContainer!.GetConnectionString(),
            ForcePathStyle = true
        };
        services.AddSingleton(s3Config);

        var s3ClientFactory = new S3ClientFactory();
        s3ClientFactory.RegisterMockClient<ExternalStorageClient>(TestBucket, _s3Client!);
        s3ClientFactory.RegisterMockClient<InternalStorageClient>(TestBucket, _s3Client!);
        services.AddSingleton<IS3ClientFactory>(s3ClientFactory);

        services.AddTransient<IBlobStorageServiceFactory, KeeperData.Infrastructure.Storage.S3BlobStorageServiceFactory>();

        // Crypto services
        services.AddSingleton<IPasswordSaltService, PasswordSaltService>();
        services.AddSingleton<IAesCryptoTransform, AesCryptoTransform>();

        // Register lineage services required by ImportReportingService
        services.AddSingleton<ILineageIdGenerator, LineageIdGenerator>();
        services.AddSingleton<ILineageMapper, LineageMapper>();
        services.AddSingleton<ILineageIndexManagerFactory, LineageIndexManagerFactory>();

        // Core services
        services.AddScoped<IImportReportingService, ImportReportingService>();
        services.AddTransient<CsvRowCounter>();
        services.AddTransient<IExternalCatalogueServiceFactory, ExternalCatalogueServiceFactory>();
        services.AddScoped<IAcquisitionPipeline, AcquisitionPipeline>();
        services.AddScoped<IIngestionPipeline, IngestionPipeline>();
        services.AddScoped<IImportOrchestrator, ImportOrchestrator>();
        services.AddScoped<IQueryService, QueryService>();
        services.AddScoped<IODataQueryService, ODataQueryService>();

        return services.BuildServiceProvider();
    }

    private async Task<string> EncryptCsvFileAsync(string csvContent)
    {
        if (_serviceProvider == null)
        {
            throw new InvalidOperationException("ServiceProvider not initialized");
        }

        var cryptoTransform = _serviceProvider.GetRequiredService<IAesCryptoTransform>();
        var passwordSaltService = _serviceProvider.GetRequiredService<IPasswordSaltService>();

        var dateStr = DateTime.UtcNow.ToString(EtlConstants.DateTimePattern);
        var fileName = $"LITP_SAMCPHHOLDING_{dateStr}.csv";
        var encryptedFileName = $"{fileName}.enc";

        var credentials = passwordSaltService.Get(encryptedFileName);
        var csvBytes = Encoding.UTF8.GetBytes(csvContent);

        _output.WriteLine($"Encrypting CSV file ({csvBytes.Length:N0} bytes)...");

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

        _output.WriteLine($"Encrypted file size: {encryptedBytes.Length:N0} bytes");

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

        var dateStr = DateTime.UtcNow.ToString(EtlConstants.DateTimePattern);
        var s3Key = $"LITP_SAMCPHHOLDING_{dateStr}.csv.enc";

        _output.WriteLine($"Uploading to S3: {s3Key} ({encryptedBytes.Length:N0} bytes)");

        await _s3Client!.PutObjectAsync(new PutObjectRequest
        {
            BucketName = TestBucket,
            Key = $"{SourcePrefix}/{s3Key}",
            InputStream = new MemoryStream(encryptedBytes),
            ContentType = "application/octet-stream"
        });

        var exists = await sourceStorage.ExistsAsync(s3Key);
        exists.Should().BeTrue($"Encrypted file should exist in S3 at key: {s3Key}");

        File.Delete(tempPath);
    }

    private async Task<Guid> StartImportAsync()
    {
        var importOrchestrator = _serviceProvider!.GetRequiredService<IImportOrchestrator>();
        var importId = Guid.NewGuid();

        _output.WriteLine($"Starting import orchestrator...");

        await importOrchestrator.StartAsync(importId, "external", CancellationToken.None);

        return importId;
    }

    private async Task VerifyImportCompletedAsync(Guid importId)
    {
        var reportingService = _serviceProvider!.GetRequiredService<IImportReportingService>();

        _output.WriteLine("Waiting for import to complete...");

        var maxWaitTime = TimeSpan.FromMinutes(5);
        var startTime = DateTime.UtcNow;
        var lastStatus = "";

        while (DateTime.UtcNow - startTime < maxWaitTime)
        {
            var report = await reportingService.GetImportReportAsync(importId, CancellationToken.None);

            if (report == null)
            {
                await Task.Delay(500);
                continue;
            }

            var currentStatus = $"{report.Status} | Acq: {report.AcquisitionPhase?.Status} | Ing: {report.IngestionPhase?.Status}";
            if (currentStatus != lastStatus)
            {
                _output.WriteLine($"  Status: {currentStatus}");
                lastStatus = currentStatus;
            }

            if (report.Status == ImportStatus.Completed)
            {
                _output.WriteLine("Import completed successfully!");

                report.AcquisitionPhase.Should().NotBeNull();
                report.AcquisitionPhase!.Status.Should().Be(PhaseStatus.Completed);
                report.AcquisitionPhase.FilesProcessed.Should().BeGreaterThan(0);

                report.IngestionPhase.Should().NotBeNull();
                report.IngestionPhase!.Status.Should().Be(PhaseStatus.Completed);
                report.IngestionPhase.RecordsCreated.Should().BeGreaterThan(0);

                _output.WriteLine($"  Files Processed: {report.AcquisitionPhase.FilesProcessed}");
                _output.WriteLine($"  Records Created: {report.IngestionPhase.RecordsCreated}");

                return;
            }

            if (report.Status == ImportStatus.Failed)
            {
                throw new Exception($"Import failed: {report.Error}");
            }

            await Task.Delay(1000);
        }

        throw new TimeoutException($"Import did not complete within {maxWaitTime.TotalSeconds} seconds");
    }

    private async Task VerifyFileProcessingAsync(Guid importId)
    {
        var reportingService = _serviceProvider!.GetRequiredService<IImportReportingService>();

        var fileReports = await reportingService.GetFileReportsAsync(importId, CancellationToken.None);

        fileReports.Should().NotBeEmpty("At least one file should have been processed");

        foreach (var fileReport in fileReports)
        {
            _output.WriteLine($"  File: {fileReport.FileName}");
            _output.WriteLine($"    Status: {fileReport.Status}");
            _output.WriteLine($"    Records Processed: {fileReport.Ingestion?.RecordsProcessed ?? 0}");
            _output.WriteLine($"    Records Created: {fileReport.Ingestion?.RecordsCreated ?? 0}");

            fileReport.Status.Should().Be(FileProcessingStatus.Ingested);
            fileReport.Ingestion.Should().NotBeNull();
            fileReport.Ingestion!.RecordsCreated.Should().BeGreaterThan(0);
        }
    }

    private async Task<List<Dictionary<string, object?>>> QueryIngestedDataAsync(string collectionName)
    {
        var queryService = _serviceProvider!.GetRequiredService<IODataQueryService>();

        _output.WriteLine($"Querying collection '{collectionName}'...");

        var result = await queryService.QueryAsync(
            collectionName: collectionName,
            filter: null,
            orderBy: "CPH asc",
            select: null,
            skip: 0,
            top: 100,
            count: true,
            cancellationToken: CancellationToken.None);

        result.Should().NotBeNull();
        result.CollectionName.Should().Be(collectionName);
        result.Data.Should().NotBeEmpty("Data should be ingested into the collection");

        _output.WriteLine($"  Query returned {result.Count} records (Total: {result.TotalCount})");

        return result.Data.ToList();
    }

    private void VerifyDataIntegrity(List<Dictionary<string, object?>> queryResults, int expectedRecordCount)
    {
        _output.WriteLine("Verifying data integrity...");

        queryResults.Count.Should().BeGreaterThan(0, "Should have ingested records");
        queryResults.Count.Should().BeLessOrEqualTo(expectedRecordCount, "Should not have more records than generated");

        var sampleSize = Math.Min(5, queryResults.Count);
        for (int i = 0; i < sampleSize; i++)
        {
            var record = queryResults[i];

            record.Should().ContainKey("CPH", "Primary key should exist");
            record.Should().ContainKey("DISEASE_TYPE", "Required field should exist");
            record.Should().ContainKey("ANIMAL_SPECIES_CODE", "Required field should exist");

            var cph = record["CPH"]?.ToString();
            cph.Should().NotBeNullOrEmpty("CPH should have a value");

            _output.WriteLine($"  Verified record: CPH={cph}");
        }

        _output.WriteLine($" Data integrity verified for {sampleSize} sample records");
    }

    #endregion
}