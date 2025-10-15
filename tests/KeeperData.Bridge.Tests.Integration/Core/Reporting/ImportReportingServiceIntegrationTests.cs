using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.Database;
using KeeperData.Core.Reporting;
using KeeperData.Core.Reporting.Dtos;
using KeeperData.Core.Reporting.Impl;
using KeeperData.Infrastructure.Database.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Core.Reporting;

/// <summary>
/// Integration tests for ImportReportingService against real MongoDB using Testcontainers.
/// Tests all functionality including import tracking, file processing, and record lineage.
/// </summary>
[Collection("MongoDB"), Trait("Dependence", "docker")]
public class ImportReportingServiceIntegrationTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly MongoDbFixture _mongoDbFixture;
    private readonly IMongoClient _mongoClient;
    private readonly IImportReportingService _reportingService;
    private readonly string _testDatabaseName = "test-import-reporting";

    public ImportReportingServiceIntegrationTests(
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

        var loggerMock = new Mock<ILogger<ImportReportingService>>();

        // Create the service under test
        _reportingService = new ImportReportingService(
            _mongoClient,
            mongoConfig,
            loggerMock.Object);
    }

    public async Task InitializeAsync()
    {
        // Clean up database before each test
        await _mongoClient.DropDatabaseAsync(_testDatabaseName);
        _testOutputHelper.WriteLine($"Initialized test database: {_testDatabaseName}");
    }

    public async Task DisposeAsync()
    {
        await _mongoClient.DropDatabaseAsync(_testDatabaseName);
    }

    #region Import-Level Tests

    [Fact]
    public async Task StartImportAsync_ShouldCreateImportReportWithInitialState()
    {
        // Arrange
        var importId = Guid.NewGuid();
        var sourceType = "External";

        // Act
        var result = await _reportingService.StartImportAsync(importId, sourceType, CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        result.ImportId.Should().Be(importId);
        result.SourceType.Should().Be(sourceType);
        result.Status.Should().Be(ImportStatus.Started);
        result.StartedAtUtc.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));
        result.CompletedAtUtc.Should().BeNull();

        result.AcquisitionPhase.Should().NotBeNull();
        result.AcquisitionPhase!.Status.Should().Be(PhaseStatus.NotStarted);
        result.AcquisitionPhase.FilesDiscovered.Should().Be(0);
        result.AcquisitionPhase.FilesProcessed.Should().Be(0);
        result.AcquisitionPhase.FilesFailed.Should().Be(0);

        result.IngestionPhase.Should().NotBeNull();
        result.IngestionPhase!.Status.Should().Be(PhaseStatus.NotStarted);
        result.IngestionPhase.FilesProcessed.Should().Be(0);
        result.IngestionPhase.RecordsCreated.Should().Be(0);
        result.IngestionPhase.RecordsUpdated.Should().Be(0);
        result.IngestionPhase.RecordsDeleted.Should().Be(0);

        _testOutputHelper.WriteLine($"Created import report for ImportId: {importId}");
    }

    [Fact]
    public async Task UpdateAcquisitionPhaseAsync_ShouldUpdateAcquisitionPhaseDetails()
    {
        // Arrange
        var importId = Guid.NewGuid();
        await _reportingService.StartImportAsync(importId, "External", CancellationToken.None);

        // Act - Start acquisition
        await _reportingService.UpdateAcquisitionPhaseAsync(importId, new AcquisitionPhaseUpdate
        {
            Status = PhaseStatus.Started,
            FilesDiscovered = 10,
            FilesProcessed = 0,
            FilesFailed = 0
        }, CancellationToken.None);

        // Assert - Started
        var report1 = await _reportingService.GetImportReportAsync(importId, CancellationToken.None);
        report1!.AcquisitionPhase!.Status.Should().Be(PhaseStatus.Started);
        report1.AcquisitionPhase.FilesDiscovered.Should().Be(10);
        report1.AcquisitionPhase.StartedAtUtc.Should().NotBeNull();

        // Act - Complete acquisition
        var completedAt = DateTime.UtcNow;
        await _reportingService.UpdateAcquisitionPhaseAsync(importId, new AcquisitionPhaseUpdate
        {
            Status = PhaseStatus.Completed,
            FilesDiscovered = 10,
            FilesProcessed = 8,
            FilesFailed = 2,
            CompletedAtUtc = completedAt
        }, CancellationToken.None);

        // Assert - Completed
        var report2 = await _reportingService.GetImportReportAsync(importId, CancellationToken.None);
        report2!.AcquisitionPhase!.Status.Should().Be(PhaseStatus.Completed);
        report2.AcquisitionPhase.FilesProcessed.Should().Be(8);
        report2.AcquisitionPhase.FilesFailed.Should().Be(2);
        report2.AcquisitionPhase.CompletedAtUtc.Should().BeCloseTo(completedAt, TimeSpan.FromSeconds(1));

        _testOutputHelper.WriteLine($"Successfully updated acquisition phase for ImportId: {importId}");
    }

    [Fact]
    public async Task UpdateIngestionPhaseAsync_ShouldUpdateIngestionPhaseDetails()
    {
        // Arrange
        var importId = Guid.NewGuid();
        await _reportingService.StartImportAsync(importId, "External", CancellationToken.None);

        // Act - Start ingestion
        await _reportingService.UpdateIngestionPhaseAsync(importId, new IngestionPhaseUpdate
        {
            Status = PhaseStatus.Started,
            FilesProcessed = 0,
            RecordsCreated = 0,
            RecordsUpdated = 0,
            RecordsDeleted = 0
        }, CancellationToken.None);

        // Assert - Started
        var report1 = await _reportingService.GetImportReportAsync(importId, CancellationToken.None);
        report1!.IngestionPhase!.Status.Should().Be(PhaseStatus.Started);
        report1.IngestionPhase.StartedAtUtc.Should().NotBeNull();

        // Act - Update progress
        await _reportingService.UpdateIngestionPhaseAsync(importId, new IngestionPhaseUpdate
        {
            Status = PhaseStatus.Started,
            FilesProcessed = 3,
            RecordsCreated = 100,
            RecordsUpdated = 50,
            RecordsDeleted = 10
        }, CancellationToken.None);

        // Assert - Progress
        var report2 = await _reportingService.GetImportReportAsync(importId, CancellationToken.None);
        report2!.IngestionPhase!.FilesProcessed.Should().Be(3);
        report2.IngestionPhase.RecordsCreated.Should().Be(100);
        report2.IngestionPhase.RecordsUpdated.Should().Be(50);
        report2.IngestionPhase.RecordsDeleted.Should().Be(10);

        // Act - Complete ingestion
        var completedAt = DateTime.UtcNow;
        await _reportingService.UpdateIngestionPhaseAsync(importId, new IngestionPhaseUpdate
        {
            Status = PhaseStatus.Completed,
            FilesProcessed = 5,
            RecordsCreated = 200,
            RecordsUpdated = 100,
            RecordsDeleted = 20,
            CompletedAtUtc = completedAt
        }, CancellationToken.None);

        // Assert - Completed
        var report3 = await _reportingService.GetImportReportAsync(importId, CancellationToken.None);
        report3!.IngestionPhase!.Status.Should().Be(PhaseStatus.Completed);
        report3.IngestionPhase.CompletedAtUtc.Should().BeCloseTo(completedAt, TimeSpan.FromSeconds(1));

        _testOutputHelper.WriteLine($"Successfully updated ingestion phase for ImportId: {importId}");
    }

    [Fact]
    public async Task CompleteImportAsync_WithSuccess_ShouldMarkImportAsCompleted()
    {
        // Arrange
        var importId = Guid.NewGuid();
        await _reportingService.StartImportAsync(importId, "External", CancellationToken.None);

        // Act
        await _reportingService.CompleteImportAsync(importId, ImportStatus.Completed, null, CancellationToken.None);

        // Assert
        var report = await _reportingService.GetImportReportAsync(importId, CancellationToken.None);
        report!.Status.Should().Be(ImportStatus.Completed);
        report.CompletedAtUtc.Should().NotBeNull();
        report.CompletedAtUtc.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));
        report.Error.Should().BeNull();

        _testOutputHelper.WriteLine($"Completed import successfully for ImportId: {importId}");
    }

    [Fact]
    public async Task CompleteImportAsync_WithFailure_ShouldMarkImportAsFailedWithError()
    {
        // Arrange
        var importId = Guid.NewGuid();
        await _reportingService.StartImportAsync(importId, "External", CancellationToken.None);
        var errorMessage = "Failed to process file due to network error";

        // Act
        await _reportingService.CompleteImportAsync(importId, ImportStatus.Failed, errorMessage, CancellationToken.None);

        // Assert
        var report = await _reportingService.GetImportReportAsync(importId, CancellationToken.None);
        report!.Status.Should().Be(ImportStatus.Failed);
        report.CompletedAtUtc.Should().NotBeNull();
        report.Error.Should().Be(errorMessage);

        _testOutputHelper.WriteLine($"Marked import as failed for ImportId: {importId} with error: {errorMessage}");
    }

    #endregion

    #region File-Level Tests

    [Fact]
    public async Task RecordFileAcquisitionAsync_ShouldStoreFileAcquisitionDetails()
    {
        // Arrange
        var importId = Guid.NewGuid();
        await _reportingService.StartImportAsync(importId, "External", CancellationToken.None);

        var fileRecord = new FileAcquisitionRecord
        {
            FileName = "test_file.csv",
            FileKey = "dest/test_file.csv",
            DatasetName = "sam_cph_holdings",
            Md5Hash = "abc123def456",
            FileSize = 1024 * 1024, // 1MB
            SourceKey = "source/test_file.csv.enc",
            DecryptionDurationMs = 1500,
            AcquiredAtUtc = DateTime.UtcNow,
            Status = FileProcessingStatus.Acquired
        };

        // Act
        await _reportingService.RecordFileAcquisitionAsync(importId, fileRecord, CancellationToken.None);

        // Assert
        var fileReports = await _reportingService.GetFileReportsAsync(importId, CancellationToken.None);
        fileReports.Should().HaveCount(1);

        var report = fileReports[0];
        report.ImportId.Should().Be(importId);
        report.FileName.Should().Be(fileRecord.FileName);
        report.FileKey.Should().Be(fileRecord.FileKey);
        report.DatasetName.Should().Be(fileRecord.DatasetName);
        report.Md5Hash.Should().Be(fileRecord.Md5Hash);
        report.FileSize.Should().Be(fileRecord.FileSize);
        report.Status.Should().Be(FileProcessingStatus.Acquired);

        report.Acquisition.Should().NotBeNull();
        report.Acquisition!.SourceKey.Should().Be(fileRecord.SourceKey);
        report.Acquisition.DecryptionDurationMs.Should().Be(fileRecord.DecryptionDurationMs);
        report.Acquisition.AcquiredAtUtc.Should().BeCloseTo(fileRecord.AcquiredAtUtc, TimeSpan.FromSeconds(1));

        _testOutputHelper.WriteLine($"Recorded file acquisition: {fileRecord.FileName}");
    }

    [Fact]
    public async Task RecordFileIngestionAsync_ShouldUpdateFileWithIngestionDetails()
    {
        // Arrange
        var importId = Guid.NewGuid();
        await _reportingService.StartImportAsync(importId, "External", CancellationToken.None);

        // Record acquisition first
        var fileKey = "dest/test_file.csv";
        await _reportingService.RecordFileAcquisitionAsync(importId, new FileAcquisitionRecord
        {
            FileName = "test_file.csv",
            FileKey = fileKey,
            DatasetName = "sam_cph_holdings",
            Md5Hash = "abc123def456",
            FileSize = 1024 * 1024,
            SourceKey = "source/test_file.csv.enc",
            DecryptionDurationMs = 1500,
            AcquiredAtUtc = DateTime.UtcNow,
            Status = FileProcessingStatus.Acquired
        }, CancellationToken.None);

        var ingestionRecord = new FileIngestionRecord
        {
            FileKey = fileKey,
            RecordsProcessed = 100,
            RecordsCreated = 60,
            RecordsUpdated = 30,
            RecordsDeleted = 10,
            IngestionDurationMs = 2500,
            IngestedAtUtc = DateTime.UtcNow,
            Status = FileProcessingStatus.Ingested
        };

        // Act
        await _reportingService.RecordFileIngestionAsync(importId, ingestionRecord, CancellationToken.None);

        // Assert
        var fileReports = await _reportingService.GetFileReportsAsync(importId, CancellationToken.None);
        fileReports.Should().HaveCount(1);

        var report = fileReports[0];
        report.Status.Should().Be(FileProcessingStatus.Ingested);

        report.Ingestion.Should().NotBeNull();
        report.Ingestion!.RecordsProcessed.Should().Be(ingestionRecord.RecordsProcessed);
        report.Ingestion.RecordsCreated.Should().Be(ingestionRecord.RecordsCreated);
        report.Ingestion.RecordsUpdated.Should().Be(ingestionRecord.RecordsUpdated);
        report.Ingestion.RecordsDeleted.Should().Be(ingestionRecord.RecordsDeleted);
        report.Ingestion.IngestionDurationMs.Should().Be(ingestionRecord.IngestionDurationMs);
        report.Ingestion.IngestedAtUtc.Should().BeCloseTo(ingestionRecord.IngestedAtUtc, TimeSpan.FromSeconds(1));

        _testOutputHelper.WriteLine($"Recorded file ingestion: {fileKey}");
    }

    [Fact]
    public async Task IsFileProcessedAsync_WithNewFile_ShouldReturnFalse()
    {
        // Arrange
        var fileKey = "dest/new_file.csv";
        var md5Hash = "newfilehash123";

        // Act
        var result = await _reportingService.IsFileProcessedAsync(fileKey, md5Hash, CancellationToken.None);

        // Assert
        result.Should().BeFalse();
        _testOutputHelper.WriteLine($"File {fileKey} with MD5 {md5Hash} is not processed (new file)");
    }

    [Fact]
    public async Task IsFileProcessedAsync_WithProcessedFile_ShouldReturnTrue()
    {
        // Arrange
        var importId = Guid.NewGuid();
        await _reportingService.StartImportAsync(importId, "External", CancellationToken.None);

        var fileKey = "dest/processed_file.csv";
        var md5Hash = "processedfilehash123";

        await _reportingService.RecordFileAcquisitionAsync(importId, new FileAcquisitionRecord
        {
            FileName = "processed_file.csv",
            FileKey = fileKey,
            DatasetName = "sam_cph_holdings",
            Md5Hash = md5Hash,
            FileSize = 1024,
            SourceKey = "source/processed_file.csv.enc",
            DecryptionDurationMs = 100,
            AcquiredAtUtc = DateTime.UtcNow,
            Status = FileProcessingStatus.Acquired
        }, CancellationToken.None);

        // Act
        var result = await _reportingService.IsFileProcessedAsync(fileKey, md5Hash, CancellationToken.None);

        // Assert
        result.Should().BeTrue();
        _testOutputHelper.WriteLine($"File {fileKey} with MD5 {md5Hash} was already processed");
    }

    [Fact]
    public async Task IsFileProcessedAsync_WithSameFileKeyButDifferentMd5_ShouldReturnFalse()
    {
        // Arrange
        var importId = Guid.NewGuid();
        await _reportingService.StartImportAsync(importId, "External", CancellationToken.None);

        var fileKey = "dest/same_name.csv";
        var md5Hash1 = "originalhash123";
        var md5Hash2 = "modifiedhash456";

        await _reportingService.RecordFileAcquisitionAsync(importId, new FileAcquisitionRecord
        {
            FileName = "same_name.csv",
            FileKey = fileKey,
            DatasetName = "sam_cph_holdings",
            Md5Hash = md5Hash1,
            FileSize = 1024,
            SourceKey = "source/same_name.csv.enc",
            DecryptionDurationMs = 100,
            AcquiredAtUtc = DateTime.UtcNow,
            Status = FileProcessingStatus.Acquired
        }, CancellationToken.None);

        // Act
        var result = await _reportingService.IsFileProcessedAsync(fileKey, md5Hash2, CancellationToken.None);

        // Assert
        result.Should().BeFalse("file content has changed (different MD5)");
        _testOutputHelper.WriteLine($"File {fileKey} has different MD5, treating as new file");
    }

    [Fact]
    public async Task RecordFileAcquisitionAsync_WithFailedStatus_ShouldStoreError()
    {
        // Arrange
        var importId = Guid.NewGuid();
        await _reportingService.StartImportAsync(importId, "External", CancellationToken.None);

        var fileRecord = new FileAcquisitionRecord
        {
            FileName = "failed_file.csv",
            FileKey = "dest/failed_file.csv",
            DatasetName = "sam_cph_holdings",
            Md5Hash = string.Empty,
            FileSize = 0,
            SourceKey = "source/failed_file.csv.enc",
            DecryptionDurationMs = 500,
            AcquiredAtUtc = DateTime.UtcNow,
            Status = FileProcessingStatus.Failed,
            Error = "Decryption failed: invalid key"
        };

        // Act
        await _reportingService.RecordFileAcquisitionAsync(importId, fileRecord, CancellationToken.None);

        // Assert
        var fileReports = await _reportingService.GetFileReportsAsync(importId, CancellationToken.None);
        var report = fileReports.Should().ContainSingle().Subject;
        report.Status.Should().Be(FileProcessingStatus.Failed);
        report.Error.Should().Be("Decryption failed: invalid key");

        _testOutputHelper.WriteLine($"Recorded failed acquisition with error: {fileRecord.Error}");
    }

    #endregion

    #region Record Lineage Tests

    [Fact]
    public async Task RecordLineageEventAsync_ShouldCreateNewLineageForRecord()
    {
        // Arrange
        var importId = Guid.NewGuid();
        var collectionName = "sam_cph_holdings";
        var recordId = "CPH001";

        var lineageEvent = new RecordLineageEvent
        {
            RecordId = recordId,
            CollectionName = collectionName,
            EventType = RecordEventType.Created,
            ImportId = importId,
            FileKey = "dest/test_file.csv",
            EventDateUtc = DateTime.UtcNow,
            ChangeType = "I",
            PreviousValues = null,
            NewValues = new BsonDocument
            {
                { "_id", recordId },
                { "FarmName", "Test Farm" },
                { "Owner", "Test Owner" }
            }
        };

        // Act
        await _reportingService.RecordLineageEventAsync(lineageEvent, CancellationToken.None);

        // Assert
        var lifecycle = await _reportingService.GetRecordLifecycleAsync(collectionName, recordId, CancellationToken.None);
        lifecycle.Should().NotBeNull();
        lifecycle!.RecordId.Should().Be(recordId);
        lifecycle.CollectionName.Should().Be(collectionName);
        lifecycle.CurrentStatus.Should().Be("Active");
        lifecycle.CreatedByImport.Should().Be(importId);
        lifecycle.LastModifiedByImport.Should().Be(importId);
        lifecycle.Events.Should().HaveCount(1);

        var event1 = lifecycle.Events[0];
        event1.EventType.Should().Be(RecordEventType.Created);
        event1.ChangeType.Should().Be("I");
        event1.NewValues.Should().NotBeNull();

        _testOutputHelper.WriteLine($"Created lineage for record {recordId}");
    }

    [Fact]
    public async Task RecordLineageEventAsync_WithMultipleEvents_ShouldAppendToLineage()
    {
        // Arrange
        var importId1 = Guid.NewGuid();
        var importId2 = Guid.NewGuid();
        var collectionName = "sam_cph_holdings";
        var recordId = "CPH002";

        // Event 1: Created
        var event1 = new RecordLineageEvent
        {
            RecordId = recordId,
            CollectionName = collectionName,
            EventType = RecordEventType.Created,
            ImportId = importId1,
            FileKey = "dest/file1.csv",
            EventDateUtc = DateTime.UtcNow.AddMinutes(-10),
            ChangeType = "I",
            PreviousValues = null,
            NewValues = new BsonDocument { { "FarmName", "Original Farm" } }
        };

        await _reportingService.RecordLineageEventAsync(event1, CancellationToken.None);

        // Event 2: Updated
        var event2 = new RecordLineageEvent
        {
            RecordId = recordId,
            CollectionName = collectionName,
            EventType = RecordEventType.Updated,
            ImportId = importId2,
            FileKey = "dest/file2.csv",
            EventDateUtc = DateTime.UtcNow,
            ChangeType = "U",
            PreviousValues = new BsonDocument { { "FarmName", "Original Farm" } },
            NewValues = new BsonDocument { { "FarmName", "Updated Farm" } }
        };

        await _reportingService.RecordLineageEventAsync(event2, CancellationToken.None);

        // Assert
        var lifecycle = await _reportingService.GetRecordLifecycleAsync(collectionName, recordId, CancellationToken.None);
        lifecycle!.Events.Should().HaveCount(2);
        lifecycle.CurrentStatus.Should().Be("Active");
        lifecycle.CreatedByImport.Should().Be(importId1);
        lifecycle.LastModifiedByImport.Should().Be(importId2);

        lifecycle.Events[0].EventType.Should().Be(RecordEventType.Created);
        lifecycle.Events[1].EventType.Should().Be(RecordEventType.Updated);

        _testOutputHelper.WriteLine($"Recorded 2 lineage events for record {recordId}");
    }

    [Fact]
    public async Task RecordLineageEventAsync_WithDeleteEvent_ShouldMarkRecordAsDeleted()
    {
        // Arrange
        var importId1 = Guid.NewGuid();
        var importId2 = Guid.NewGuid();
        var collectionName = "sam_cph_holdings";
        var recordId = "CPH003";

        // Create record
        await _reportingService.RecordLineageEventAsync(new RecordLineageEvent
        {
            RecordId = recordId,
            CollectionName = collectionName,
            EventType = RecordEventType.Created,
            ImportId = importId1,
            FileKey = "dest/file1.csv",
            EventDateUtc = DateTime.UtcNow.AddMinutes(-10),
            ChangeType = "I",
            PreviousValues = null,
            NewValues = new BsonDocument { { "FarmName", "Test Farm" } }
        }, CancellationToken.None);

        // Delete record
        await _reportingService.RecordLineageEventAsync(new RecordLineageEvent
        {
            RecordId = recordId,
            CollectionName = collectionName,
            EventType = RecordEventType.Deleted,
            ImportId = importId2,
            FileKey = "dest/file2.csv",
            EventDateUtc = DateTime.UtcNow,
            ChangeType = "D",
            PreviousValues = new BsonDocument { { "FarmName", "Test Farm" } },
            NewValues = null
        }, CancellationToken.None);

        // Assert
        var lifecycle = await _reportingService.GetRecordLifecycleAsync(collectionName, recordId, CancellationToken.None);
        lifecycle!.CurrentStatus.Should().Be("Deleted");
        lifecycle.Events.Should().HaveCount(2);
        lifecycle.Events[1].EventType.Should().Be(RecordEventType.Deleted);

        _testOutputHelper.WriteLine($"Marked record {recordId} as deleted in lineage");
    }

    [Fact]
    public async Task RecordLineageEventsBatchAsync_ShouldRecordMultipleEventsEfficiently()
    {
        // Arrange
        var importId = Guid.NewGuid();
        var collectionName = "sam_cph_holdings";
        var eventTime = DateTime.UtcNow;

        var events = Enumerable.Range(1, 100).Select(i => new RecordLineageEvent
        {
            RecordId = $"CPH{i:D6}",
            CollectionName = collectionName,
            EventType = RecordEventType.Created,
            ImportId = importId,
            FileKey = "dest/bulk_file.csv",
            EventDateUtc = eventTime,
            ChangeType = "I",
            PreviousValues = null,
            NewValues = new BsonDocument { { "FarmName", $"Farm {i}" } }
        }).ToList();

        // Act
        await _reportingService.RecordLineageEventsBatchAsync(events, CancellationToken.None);

        // Assert - Verify a few records
        var lifecycle1 = await _reportingService.GetRecordLifecycleAsync(collectionName, "CPH000001", CancellationToken.None);
        lifecycle1.Should().NotBeNull();
        lifecycle1!.Events.Should().HaveCount(1);

        var lifecycle50 = await _reportingService.GetRecordLifecycleAsync(collectionName, "CPH000050", CancellationToken.None);
        lifecycle50.Should().NotBeNull();
        lifecycle50!.Events.Should().HaveCount(1);

        var lifecycle100 = await _reportingService.GetRecordLifecycleAsync(collectionName, "CPH000100", CancellationToken.None);
        lifecycle100.Should().NotBeNull();
        lifecycle100!.Events.Should().HaveCount(1);

        _testOutputHelper.WriteLine($"Recorded {events.Count} lineage events in batch");
    }

    [Fact]
    public async Task GetRecordLineageAsync_ShouldReturnEventsInOrder()
    {
        // Arrange
        var importId = Guid.NewGuid();
        var collectionName = "sam_cph_holdings";
        var recordId = "CPH004";

        var events = new[]
        {
            new RecordLineageEvent
            {
                RecordId = recordId,
                CollectionName = collectionName,
                EventType = RecordEventType.Created,
                ImportId = importId,
                FileKey = "dest/file1.csv",
                EventDateUtc = DateTime.UtcNow.AddMinutes(-20),
                ChangeType = "I",
                PreviousValues = null,
                NewValues = new BsonDocument { { "Version", "1" } }
            },
            new RecordLineageEvent
            {
                RecordId = recordId,
                CollectionName = collectionName,
                EventType = RecordEventType.Updated,
                ImportId = importId,
                FileKey = "dest/file2.csv",
                EventDateUtc = DateTime.UtcNow.AddMinutes(-10),
                ChangeType = "U",
                PreviousValues = new BsonDocument { { "Version", "1" } },
                NewValues = new BsonDocument { { "Version", "2" } }
            },
            new RecordLineageEvent
            {
                RecordId = recordId,
                CollectionName = collectionName,
                EventType = RecordEventType.Updated,
                ImportId = importId,
                FileKey = "dest/file3.csv",
                EventDateUtc = DateTime.UtcNow,
                ChangeType = "U",
                PreviousValues = new BsonDocument { { "Version", "2" } },
                NewValues = new BsonDocument { { "Version", "3" } }
            }
        };

        foreach (var evt in events)
        {
            await _reportingService.RecordLineageEventAsync(evt, CancellationToken.None);
        }

        // Act
        var lineage = await _reportingService.GetRecordLineageAsync(collectionName, recordId, CancellationToken.None);

        // Assert
        lineage.Should().HaveCount(3);
        lineage[0].EventType.Should().Be(RecordEventType.Created);
        lineage[1].EventType.Should().Be(RecordEventType.Updated);
        lineage[2].EventType.Should().Be(RecordEventType.Updated);

        // Verify timestamps are in order
        lineage[0].EventDateUtc.Should().BeBefore(lineage[1].EventDateUtc);
        lineage[1].EventDateUtc.Should().BeBefore(lineage[2].EventDateUtc);

        _testOutputHelper.WriteLine($"Retrieved {lineage.Count} lineage events in chronological order");
    }

    [Fact]
    public async Task GetRecordLifecycleAsync_ForNonExistentRecord_ShouldReturnNull()
    {
        // Act
        var lifecycle = await _reportingService.GetRecordLifecycleAsync("sam_cph_holdings", "NONEXISTENT", CancellationToken.None);

        // Assert
        lifecycle.Should().BeNull();
        _testOutputHelper.WriteLine("Non-existent record returned null lifecycle");
    }

    #endregion

    #region End-to-End Scenario Tests

    [Fact]
    public async Task CompleteImportScenario_ShouldTrackAllAspectsCorrectly()
    {
        // Arrange
        var importId = Guid.NewGuid();
        var sourceType = "External";

        // Act 1: Start import
        await _reportingService.StartImportAsync(importId, sourceType, CancellationToken.None);

        // Act 2: Acquisition phase
        await _reportingService.UpdateAcquisitionPhaseAsync(importId, new AcquisitionPhaseUpdate
        {
            Status = PhaseStatus.Started,
            FilesDiscovered = 3,
            FilesProcessed = 0,
            FilesFailed = 0
        }, CancellationToken.None);

        // Record 3 file acquisitions
        for (int i = 1; i <= 3; i++)
        {
            await _reportingService.RecordFileAcquisitionAsync(importId, new FileAcquisitionRecord
            {
                FileName = $"file{i}.csv",
                FileKey = $"dest/file{i}.csv",
                DatasetName = "sam_cph_holdings",
                Md5Hash = $"hash{i}",
                FileSize = 1024 * i,
                SourceKey = $"source/file{i}.csv.enc",
                DecryptionDurationMs = 100 * i,
                AcquiredAtUtc = DateTime.UtcNow,
                Status = FileProcessingStatus.Acquired
            }, CancellationToken.None);
        }

        await _reportingService.UpdateAcquisitionPhaseAsync(importId, new AcquisitionPhaseUpdate
        {
            Status = PhaseStatus.Completed,
            FilesDiscovered = 3,
            FilesProcessed = 3,
            FilesFailed = 0,
            CompletedAtUtc = DateTime.UtcNow
        }, CancellationToken.None);

        // Act 3: Ingestion phase
        await _reportingService.UpdateIngestionPhaseAsync(importId, new IngestionPhaseUpdate
        {
            Status = PhaseStatus.Started,
            FilesProcessed = 0,
            RecordsCreated = 0,
            RecordsUpdated = 0,
            RecordsDeleted = 0
        }, CancellationToken.None);

        // Record ingestion for each file
        var totalCreated = 0;
        var totalUpdated = 0;
        var totalDeleted = 0;

        for (int i = 1; i <= 3; i++)
        {
            var created = 10 * i;
            var updated = 5 * i;
            var deleted = 2 * i;

            totalCreated += created;
            totalUpdated += updated;
            totalDeleted += deleted;

            await _reportingService.RecordFileIngestionAsync(importId, new FileIngestionRecord
            {
                FileKey = $"dest/file{i}.csv",
                RecordsProcessed = created + updated + deleted,
                RecordsCreated = created,
                RecordsUpdated = updated,
                RecordsDeleted = deleted,
                IngestionDurationMs = 500 * i,
                IngestedAtUtc = DateTime.UtcNow,
                Status = FileProcessingStatus.Ingested
            }, CancellationToken.None);

            // Record some lineage events
            for (int j = 1; j <= created; j++)
            {
                await _reportingService.RecordLineageEventAsync(new RecordLineageEvent
                {
                    RecordId = $"CPH{i:D3}{j:D3}",
                    CollectionName = "sam_cph_holdings",
                    EventType = RecordEventType.Created,
                    ImportId = importId,
                    FileKey = $"dest/file{i}.csv",
                    EventDateUtc = DateTime.UtcNow,
                    ChangeType = "I",
                    PreviousValues = null,
                    NewValues = new BsonDocument { { "FarmName", $"Farm {i}-{j}" } }
                }, CancellationToken.None);
            }
        }

        await _reportingService.UpdateIngestionPhaseAsync(importId, new IngestionPhaseUpdate
        {
            Status = PhaseStatus.Completed,
            FilesProcessed = 3,
            RecordsCreated = totalCreated,
            RecordsUpdated = totalUpdated,
            RecordsDeleted = totalDeleted,
            CompletedAtUtc = DateTime.UtcNow
        }, CancellationToken.None);

        // Act 4: Complete import
        await _reportingService.CompleteImportAsync(importId, ImportStatus.Completed, null, CancellationToken.None);

        // Assert: Verify complete import report
        var importReport = await _reportingService.GetImportReportAsync(importId, CancellationToken.None);
        importReport.Should().NotBeNull();
        importReport!.Status.Should().Be(ImportStatus.Completed);

        importReport.AcquisitionPhase!.Status.Should().Be(PhaseStatus.Completed);
        importReport.AcquisitionPhase.FilesDiscovered.Should().Be(3);
        importReport.AcquisitionPhase.FilesProcessed.Should().Be(3);
        importReport.AcquisitionPhase.FilesFailed.Should().Be(0);

        importReport.IngestionPhase!.Status.Should().Be(PhaseStatus.Completed);
        importReport.IngestionPhase.FilesProcessed.Should().Be(3);
        importReport.IngestionPhase.RecordsCreated.Should().Be(totalCreated);
        importReport.IngestionPhase.RecordsUpdated.Should().Be(totalUpdated);
        importReport.IngestionPhase.RecordsDeleted.Should().Be(totalDeleted);

        // Verify file reports
        var fileReports = await _reportingService.GetFileReportsAsync(importId, CancellationToken.None);
        fileReports.Should().HaveCount(3);
        fileReports.Should().OnlyContain(f => f.Status == FileProcessingStatus.Ingested);
        fileReports.Should().OnlyContain(f => f.Acquisition != null && f.Ingestion != null);

        // Verify lineage for a sample record
        var sampleLifecycle = await _reportingService.GetRecordLifecycleAsync("sam_cph_holdings", "CPH001001", CancellationToken.None);
        sampleLifecycle.Should().NotBeNull();
        sampleLifecycle!.Events.Should().HaveCount(1);
        sampleLifecycle.CreatedByImport.Should().Be(importId);

        _testOutputHelper.WriteLine($"Complete import scenario validated successfully");
        _testOutputHelper.WriteLine($"- Files: {fileReports.Count}");
        _testOutputHelper.WriteLine($"- Records Created: {totalCreated}");
        _testOutputHelper.WriteLine($"- Records Updated: {totalUpdated}");
        _testOutputHelper.WriteLine($"- Records Deleted: {totalDeleted}");
    }

    #endregion
}
