using FluentAssertions;
using KeeperData.Core.Reporting;
using KeeperData.Core.Reporting.Dtos;

namespace KeeperData.Core.Tests.Unit.Reporting.Dtos;

public class StartBulkImportResponseTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var importId = Guid.NewGuid();

        var response = new StartBulkImportResponse
        {
            ImportId = importId,
            SourceType = "external",
            Message = "Import started successfully"
        };

        response.ImportId.Should().Be(importId);
        response.SourceType.Should().Be("external");
        response.Message.Should().Be("Import started successfully");
    }

    [Fact]
    public void StartedAt_HasDefaultValue()
    {
        var response = new StartBulkImportResponse
        {
            ImportId = Guid.NewGuid(),
            SourceType = "internal",
            Message = "OK"
        };

        response.StartedAt.Should().Be(default);
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var importId = Guid.NewGuid();
        var startedAt = DateTime.UtcNow;

        var response = new StartBulkImportResponse
        {
            ImportId = importId,
            SourceType = "external",
            Message = "Bulk import initiated for 15 files",
            StartedAt = startedAt
        };

        response.ImportId.Should().Be(importId);
        response.SourceType.Should().Be("external");
        response.Message.Should().Contain("15 files");
        response.StartedAt.Should().Be(startedAt);
    }

    [Fact]
    public void Record_SupportsEquality()
    {
        var importId = Guid.NewGuid();
        var response1 = new StartBulkImportResponse { ImportId = importId, SourceType = "internal", Message = "OK" };
        var response2 = new StartBulkImportResponse { ImportId = importId, SourceType = "internal", Message = "OK" };
        var response3 = new StartBulkImportResponse { ImportId = Guid.NewGuid(), SourceType = "internal", Message = "OK" };

        response1.Should().Be(response2);
        response1.Should().NotBe(response3);
    }
}

public class FileIngestionMetricsTests
{
    [Fact]
    public void DefaultValues_AreZero()
    {
        var metrics = new FileIngestionMetrics();

        metrics.RecordsProcessed.Should().Be(0);
        metrics.RecordsCreated.Should().Be(0);
        metrics.RecordsUpdated.Should().Be(0);
        metrics.RecordsDeleted.Should().Be(0);
        metrics.S3DownloadDurationMs.Should().Be(0);
        metrics.MongoIngestionDurationMs.Should().Be(0);
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var metrics = new FileIngestionMetrics
        {
            RecordsProcessed = 1000,
            RecordsCreated = 600,
            RecordsUpdated = 350,
            RecordsDeleted = 50,
            S3DownloadDurationMs = 5000,
            MongoIngestionDurationMs = 15000
        };

        metrics.RecordsProcessed.Should().Be(1000);
        metrics.RecordsCreated.Should().Be(600);
        metrics.RecordsUpdated.Should().Be(350);
        metrics.RecordsDeleted.Should().Be(50);
        metrics.S3DownloadDurationMs.Should().Be(5000);
        metrics.MongoIngestionDurationMs.Should().Be(15000);
    }

    [Fact]
    public void Record_SupportsWithExpression()
    {
        var original = new FileIngestionMetrics { RecordsProcessed = 100 };
        var modified = original with { RecordsCreated = 50, RecordsUpdated = 30 };

        modified.RecordsProcessed.Should().Be(100);
        modified.RecordsCreated.Should().Be(50);
        modified.RecordsUpdated.Should().Be(30);
    }
}

public class ImportSummaryTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var importId = Guid.NewGuid();

        var summary = new ImportSummary
        {
            ImportId = importId,
            Status = ImportStatus.Completed
        };

        summary.ImportId.Should().Be(importId);
        summary.Status.Should().Be(ImportStatus.Completed);
    }

    [Fact]
    public void OptionalProperties_HaveDefaultValues()
    {
        var summary = new ImportSummary
        {
            ImportId = Guid.NewGuid(),
            Status = ImportStatus.Started
        };

        summary.StartedAtUtc.Should().Be(default);
        summary.CompletedAtUtc.Should().BeNull();
        summary.FilesProcessed.Should().Be(0);
        summary.RecordsCreated.Should().Be(0);
        summary.RecordsUpdated.Should().Be(0);
        summary.RecordsDeleted.Should().Be(0);
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var importId = Guid.NewGuid();
        var startedAt = DateTime.UtcNow.AddMinutes(-30);
        var completedAt = DateTime.UtcNow;

        var summary = new ImportSummary
        {
            ImportId = importId,
            Status = ImportStatus.Completed,
            StartedAtUtc = startedAt,
            CompletedAtUtc = completedAt,
            FilesProcessed = 15,
            RecordsCreated = 5000,
            RecordsUpdated = 3000,
            RecordsDeleted = 200
        };

        summary.ImportId.Should().Be(importId);
        summary.Status.Should().Be(ImportStatus.Completed);
        summary.StartedAtUtc.Should().Be(startedAt);
        summary.CompletedAtUtc.Should().Be(completedAt);
        summary.FilesProcessed.Should().Be(15);
        summary.RecordsCreated.Should().Be(5000);
        summary.RecordsUpdated.Should().Be(3000);
        summary.RecordsDeleted.Should().Be(200);
    }

    [Theory]
    [InlineData(ImportStatus.Started)]
    [InlineData(ImportStatus.Completed)]
    [InlineData(ImportStatus.Failed)]
    public void Status_CanBeAnyValidValue(ImportStatus status)
    {
        var summary = new ImportSummary
        {
            ImportId = Guid.NewGuid(),
            Status = status
        };

        summary.Status.Should().Be(status);
    }
}

public class RecordLifecycleTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var createdBy = Guid.NewGuid();
        var lastModifiedBy = Guid.NewGuid();
        var events = new List<RecordLineageEvent>();

        var lifecycle = new RecordLifecycle
        {
            RecordId = "rec-123",
            CollectionName = "users",
            CurrentStatus = "Active",
            CreatedByImport = createdBy,
            LastModifiedByImport = lastModifiedBy,
            Events = events
        };

        lifecycle.RecordId.Should().Be("rec-123");
        lifecycle.CollectionName.Should().Be("users");
        lifecycle.CurrentStatus.Should().Be("Active");
        lifecycle.CreatedByImport.Should().Be(createdBy);
        lifecycle.LastModifiedByImport.Should().Be(lastModifiedBy);
        lifecycle.Events.Should().BeEmpty();
    }

    [Fact]
    public void OptionalProperties_HaveDefaultValues()
    {
        var lifecycle = new RecordLifecycle
        {
            RecordId = "rec-123",
            CollectionName = "users",
            CurrentStatus = "Active",
            CreatedByImport = Guid.NewGuid(),
            LastModifiedByImport = Guid.NewGuid(),
            Events = []
        };

        lifecycle.CreatedAtUtc.Should().Be(default);
        lifecycle.LastModifiedAtUtc.Should().Be(default);
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var createdBy = Guid.NewGuid();
        var lastModifiedBy = Guid.NewGuid();
        var createdAt = DateTime.UtcNow.AddDays(-30);
        var lastModifiedAt = DateTime.UtcNow;

        var events = new List<RecordLineageEvent>
        {
            new()
            {
                RecordId = "rec-123",
                CollectionName = "users",
                EventType = RecordEventType.Created,
                ImportId = createdBy,
                FileKey = "file1.csv",
                ChangeType = "Insert"
            }
        };

        var lifecycle = new RecordLifecycle
        {
            RecordId = "rec-123",
            CollectionName = "users",
            CurrentStatus = "Active",
            CreatedByImport = createdBy,
            LastModifiedByImport = lastModifiedBy,
            CreatedAtUtc = createdAt,
            LastModifiedAtUtc = lastModifiedAt,
            Events = events
        };

        lifecycle.CreatedAtUtc.Should().Be(createdAt);
        lifecycle.LastModifiedAtUtc.Should().Be(lastModifiedAt);
        lifecycle.Events.Should().ContainSingle();
    }
}

public class RecordLineageEventTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var importId = Guid.NewGuid();

        var lineageEvent = new RecordLineageEvent
        {
            RecordId = "rec-456",
            CollectionName = "orders",
            EventType = RecordEventType.Updated,
            ImportId = importId,
            FileKey = "delta_2024-06-15.csv",
            ChangeType = "Update"
        };

        lineageEvent.RecordId.Should().Be("rec-456");
        lineageEvent.CollectionName.Should().Be("orders");
        lineageEvent.EventType.Should().Be(RecordEventType.Updated);
        lineageEvent.ImportId.Should().Be(importId);
        lineageEvent.FileKey.Should().Be("delta_2024-06-15.csv");
        lineageEvent.ChangeType.Should().Be("Update");
    }

    [Fact]
    public void OptionalProperties_HaveDefaultValues()
    {
        var lineageEvent = new RecordLineageEvent
        {
            RecordId = "rec-456",
            CollectionName = "orders",
            EventType = RecordEventType.Created,
            ImportId = Guid.NewGuid(),
            FileKey = "file.csv",
            ChangeType = "Insert"
        };

        lineageEvent.EventDateUtc.Should().Be(default);
        lineageEvent.PreviousValues.Should().BeNull();
        lineageEvent.NewValues.Should().BeNull();
    }

    [Theory]
    [InlineData(RecordEventType.Created)]
    [InlineData(RecordEventType.Updated)]
    [InlineData(RecordEventType.Deleted)]
    public void EventType_CanBeAnyValidValue(RecordEventType eventType)
    {
        var lineageEvent = new RecordLineageEvent
        {
            RecordId = "rec-789",
            CollectionName = "products",
            EventType = eventType,
            ImportId = Guid.NewGuid(),
            FileKey = "file.csv",
            ChangeType = eventType.ToString()
        };

        lineageEvent.EventType.Should().Be(eventType);
    }
}

public class PaginatedLineageEventsTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var events = new List<RecordLineageEvent>();

        var paginated = new PaginatedLineageEvents
        {
            RecordId = "rec-123",
            CollectionName = "users",
            CurrentStatus = "Active",
            Events = events
        };

        paginated.RecordId.Should().Be("rec-123");
        paginated.CollectionName.Should().Be("users");
        paginated.CurrentStatus.Should().Be("Active");
        paginated.Events.Should().BeEmpty();
    }

    [Fact]
    public void PaginationProperties_HaveDefaultValues()
    {
        var paginated = new PaginatedLineageEvents
        {
            RecordId = "rec-123",
            CollectionName = "users",
            CurrentStatus = "Active",
            Events = []
        };

        paginated.TotalEvents.Should().Be(0);
        paginated.Skip.Should().Be(0);
        paginated.Top.Should().Be(0);
        paginated.Count.Should().Be(0);
    }

    [Fact]
    public void PaginationProperties_CanBeSet()
    {
        var paginated = new PaginatedLineageEvents
        {
            RecordId = "rec-123",
            CollectionName = "users",
            CurrentStatus = "Active",
            TotalEvents = 100,
            Skip = 20,
            Top = 10,
            Count = 10,
            Events = []
        };

        paginated.TotalEvents.Should().Be(100);
        paginated.Skip.Should().Be(20);
        paginated.Top.Should().Be(10);
        paginated.Count.Should().Be(10);
    }
}

public class IngestionPhaseUpdateTests
{
    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var update = new IngestionPhaseUpdate();

        update.Status.Should().Be(default(PhaseStatus));
        update.FilesProcessed.Should().Be(0);
        update.RecordsCreated.Should().Be(0);
        update.RecordsUpdated.Should().Be(0);
        update.RecordsDeleted.Should().Be(0);
        update.CompletedAtUtc.Should().BeNull();
        update.CurrentFileStatus.Should().BeNull();
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var completedAt = DateTime.UtcNow;
        var currentFileStatus = new IngestionCurrentFileStatus
        {
            FileName = "test.csv",
            TotalRows = 1000,
            RowNumber = 100,
            PercentageCompleted = 10
        };

        var update = new IngestionPhaseUpdate
        {
            Status = PhaseStatus.Started,
            FilesProcessed = 5,
            RecordsCreated = 500,
            RecordsUpdated = 200,
            RecordsDeleted = 50,
            CompletedAtUtc = completedAt,
            CurrentFileStatus = currentFileStatus
        };

        update.Status.Should().Be(PhaseStatus.Started);
        update.FilesProcessed.Should().Be(5);
        update.RecordsCreated.Should().Be(500);
        update.RecordsUpdated.Should().Be(200);
        update.RecordsDeleted.Should().Be(50);
        update.CompletedAtUtc.Should().Be(completedAt);
        update.CurrentFileStatus.Should().Be(currentFileStatus);
    }
}

public class AcquisitionPhaseUpdateTests
{
    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var update = new AcquisitionPhaseUpdate();

        update.Status.Should().Be(default(PhaseStatus));
        update.FilesDiscovered.Should().Be(0);
        update.FilesProcessed.Should().Be(0);
        update.FilesFailed.Should().Be(0);
        update.CompletedAtUtc.Should().BeNull();
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var completedAt = DateTime.UtcNow;

        var update = new AcquisitionPhaseUpdate
        {
            Status = PhaseStatus.Completed,
            FilesDiscovered = 20,
            FilesProcessed = 18,
            FilesFailed = 2,
            CompletedAtUtc = completedAt
        };

        update.Status.Should().Be(PhaseStatus.Completed);
        update.FilesDiscovered.Should().Be(20);
        update.FilesProcessed.Should().Be(18);
        update.FilesFailed.Should().Be(2);
        update.CompletedAtUtc.Should().Be(completedAt);
    }
}

public class ErrorResponseTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var response = new ErrorResponse { Message = "Something went wrong" };

        response.Message.Should().Be("Something went wrong");
    }

    [Fact]
    public void OptionalProperties_HaveDefaultValues()
    {
        var response = new ErrorResponse { Message = "Error" };

        response.Timestamp.Should().Be(default);
        response.ImportId.Should().BeNull();
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var timestamp = DateTime.UtcNow;
        var importId = Guid.NewGuid();

        var response = new ErrorResponse
        {
            Message = "Import failed: file not found",
            Timestamp = timestamp,
            ImportId = importId
        };

        response.Message.Should().Contain("file not found");
        response.Timestamp.Should().Be(timestamp);
        response.ImportId.Should().Be(importId);
    }
}

public class DeleteCollectionResponseTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var response = new DeleteCollectionResponse
        {
            CollectionName = "users",
            Success = true,
            Message = "Deleted successfully"
        };

        response.CollectionName.Should().Be("users");
        response.Success.Should().BeTrue();
        response.Message.Should().Be("Deleted successfully");
    }

    [Fact]
    public void OptionalProperties_HaveDefaultValues()
    {
        var response = new DeleteCollectionResponse
        {
            CollectionName = "test",
            Success = true,
            Message = "OK"
        };

        response.DeletedAtUtc.Should().Be(default);
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var deletedAt = DateTime.UtcNow;

        var response = new DeleteCollectionResponse
        {
            CollectionName = "orders",
            Success = true,
            Message = "Collection 'orders' deleted",
            DeletedAtUtc = deletedAt
        };

        response.CollectionName.Should().Be("orders");
        response.Success.Should().BeTrue();
        response.DeletedAtUtc.Should().Be(deletedAt);
    }
}

public class DeleteCollectionsResponseTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var deletedCollections = new List<string> { "users", "orders" };

        var response = new DeleteCollectionsResponse
        {
            DeletedCollections = deletedCollections,
            TotalCount = 2,
            Success = true,
            Message = "Deleted 2 collections"
        };

        response.DeletedCollections.Should().HaveCount(2);
        response.TotalCount.Should().Be(2);
        response.Success.Should().BeTrue();
    }

    [Fact]
    public void OptionalProperties_HaveDefaultValues()
    {
        var response = new DeleteCollectionsResponse
        {
            DeletedCollections = [],
            TotalCount = 0,
            Success = true,
            Message = "No collections to delete"
        };

        response.DeletedAtUtc.Should().Be(default);
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var deletedAt = DateTime.UtcNow;

        var response = new DeleteCollectionsResponse
        {
            DeletedCollections = new List<string> { "a", "b", "c" },
            TotalCount = 3,
            Success = true,
            Message = "All collections deleted",
            DeletedAtUtc = deletedAt
        };

        response.DeletedCollections.Should().HaveCount(3);
        response.TotalCount.Should().Be(3);
        response.DeletedAtUtc.Should().Be(deletedAt);
    }
}

public class FileAcquisitionRecordTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var record = new FileAcquisitionRecord
        {
            FileName = "data_2024-06-15.csv",
            FileKey = "folder/data_2024-06-15.csv",
            DatasetName = "users",
            ETag = "abc123",
            SourceKey = "external/data.csv"
        };

        record.FileName.Should().Be("data_2024-06-15.csv");
        record.FileKey.Should().Be("folder/data_2024-06-15.csv");
        record.DatasetName.Should().Be("users");
        record.ETag.Should().Be("abc123");
        record.SourceKey.Should().Be("external/data.csv");
    }

    [Fact]
    public void OptionalProperties_HaveDefaultValues()
    {
        var record = new FileAcquisitionRecord
        {
            FileName = "test.csv",
            FileKey = "test.csv",
            DatasetName = "test",
            ETag = "123",
            SourceKey = "src/test.csv"
        };

        record.FileSize.Should().Be(0);
        record.DecryptionDurationMs.Should().Be(0);
        record.AcquiredAtUtc.Should().Be(default);
        record.Status.Should().Be(default(FileProcessingStatus));
        record.Error.Should().BeNull();
    }


    [Fact]
    public void AllProperties_CanBeSet()
    {
        var acquiredAt = DateTime.UtcNow;

        var record = new FileAcquisitionRecord
        {
            FileName = "data.csv",
            FileKey = "folder/data.csv",
            DatasetName = "customers",
            ETag = "etag123",
            FileSize = 1024000,
            SourceKey = "external/data.csv",
            DecryptionDurationMs = 500,
            AcquiredAtUtc = acquiredAt,
            Status = FileProcessingStatus.Acquired,
            Error = null
        };

        record.FileSize.Should().Be(1024000);
        record.DecryptionDurationMs.Should().Be(500);
        record.AcquiredAtUtc.Should().Be(acquiredAt);
        record.Status.Should().Be(FileProcessingStatus.Acquired);
    }
}

public class FileIngestionRecordTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var record = new FileIngestionRecord { FileKey = "folder/data.csv" };

        record.FileKey.Should().Be("folder/data.csv");
    }

    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var record = new FileIngestionRecord { FileKey = "test.csv" };

        record.RecordsProcessed.Should().Be(0);
        record.RecordsCreated.Should().Be(0);
        record.RecordsUpdated.Should().Be(0);
        record.RecordsDeleted.Should().Be(0);
        record.IngestionDurationMs.Should().Be(0);
        record.AverageRecordIngestionMs.Should().Be(0);
        record.S3DownloadDurationMs.Should().Be(0);
        record.MongoIngestionDurationMs.Should().Be(0);
        record.IngestedAtUtc.Should().Be(default);
        record.Status.Should().Be(default(FileProcessingStatus));
        record.Error.Should().BeNull();
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var ingestedAt = DateTime.UtcNow;


        var record = new FileIngestionRecord
        {
            FileKey = "folder/data.csv",
            RecordsProcessed = 1000,
            RecordsCreated = 600,
            RecordsUpdated = 350,
            RecordsDeleted = 50,
            IngestionDurationMs = 30000,
            AverageRecordIngestionMs = 30.0,
            S3DownloadDurationMs = 5000,
            MongoIngestionDurationMs = 25000,
            IngestedAtUtc = ingestedAt,
            Status = FileProcessingStatus.Ingested,
            Error = null
        };

        record.RecordsProcessed.Should().Be(1000);
        record.RecordsCreated.Should().Be(600);
        record.RecordsUpdated.Should().Be(350);
        record.RecordsDeleted.Should().Be(50);
        record.IngestionDurationMs.Should().Be(30000);
        record.AverageRecordIngestionMs.Should().Be(30.0);
        record.S3DownloadDurationMs.Should().Be(5000);
        record.MongoIngestionDurationMs.Should().Be(25000);
        record.IngestedAtUtc.Should().Be(ingestedAt);
        record.Status.Should().Be(FileProcessingStatus.Ingested);
    }

    [Fact]
    public void CanRepresentFailedIngestion()
    {
        var record = new FileIngestionRecord
        {
            FileKey = "broken.csv",
            Status = FileProcessingStatus.Failed,
            Error = "CSV parsing failed at row 42"
        };

        record.Status.Should().Be(FileProcessingStatus.Failed);
        record.Error.Should().Contain("row 42");
    }
}

public class AcquisitionDetailsTests
{
    [Fact]
    public void RequiredProperties_MustBeSet()
    {
        var details = new AcquisitionDetails { SourceKey = "external/data.csv" };

        details.SourceKey.Should().Be("external/data.csv");
    }

    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var details = new AcquisitionDetails { SourceKey = "test.csv" };

        details.AcquiredAtUtc.Should().Be(default);
        details.DecryptionDurationMs.Should().Be(0);
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var acquiredAt = DateTime.UtcNow;

        var details = new AcquisitionDetails
        {
            AcquiredAtUtc = acquiredAt,
            SourceKey = "s3://bucket/folder/encrypted.csv",
            DecryptionDurationMs = 1500
        };

        details.AcquiredAtUtc.Should().Be(acquiredAt);
        details.SourceKey.Should().Contain("encrypted.csv");
        details.DecryptionDurationMs.Should().Be(1500);
    }
}

public class IngestionDetailsTests
{
    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var details = new IngestionDetails();

        details.IngestedAtUtc.Should().Be(default);
        details.RecordsProcessed.Should().Be(0);
        details.RecordsCreated.Should().Be(0);
    }
}
