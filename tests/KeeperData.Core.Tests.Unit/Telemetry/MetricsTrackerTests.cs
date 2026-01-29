using FluentAssertions;
using KeeperData.Core.Reporting.Dtos;
using KeeperData.Core.Telemetry;

namespace KeeperData.Core.Tests.Unit.Telemetry;

public class BatchMetricsTrackerTests
{
    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var tracker = new BatchMetricsTracker();

        tracker.BatchId.Should().BeEmpty();
        tracker.StartTime.Should().Be(default);
        tracker.ProcessedRecords.Should().Be(0);
        tracker.FailedRecords.Should().Be(0);
        tracker.ErrorMessage.Should().BeNull();
        tracker.Processed.Should().Be(0);
        tracker.Created.Should().Be(0);
        tracker.Updated.Should().Be(0);
        tracker.Deleted.Should().Be(0);
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var startTime = DateTime.UtcNow.AddMinutes(-5);

        var tracker = new BatchMetricsTracker
        {
            BatchId = "batch-001",
            StartTime = startTime,
            ProcessedRecords = 100,
            FailedRecords = 5,
            ErrorMessage = "Some records failed validation",
            Processed = 95,
            Created = 50,
            Updated = 40,
            Deleted = 5
        };

        tracker.BatchId.Should().Be("batch-001");
        tracker.StartTime.Should().Be(startTime);
        tracker.ProcessedRecords.Should().Be(100);
        tracker.FailedRecords.Should().Be(5);
        tracker.ErrorMessage.Should().Be("Some records failed validation");
        tracker.Processed.Should().Be(95);
        tracker.Created.Should().Be(50);
        tracker.Updated.Should().Be(40);
        tracker.Deleted.Should().Be(5);
    }

    [Fact]
    public void ElapsedMilliseconds_CalculatesFromStartTime()
    {
        var tracker = new BatchMetricsTracker
        {
            StartTime = DateTime.UtcNow.AddSeconds(-2)
        };

        tracker.ElapsedMilliseconds.Should().BeGreaterThanOrEqualTo(1900);
        tracker.ElapsedMilliseconds.Should().BeLessThan(3000);
    }

    [Fact]
    public void HasErrors_ReturnsTrueWhenFailedRecordsGreaterThanZero()
    {
        var tracker = new BatchMetricsTracker { FailedRecords = 1 };

        tracker.HasErrors.Should().BeTrue();
    }

    [Fact]
    public void HasErrors_ReturnsTrueWhenErrorMessageIsSet()
    {
        var tracker = new BatchMetricsTracker { ErrorMessage = "Error occurred" };

        tracker.HasErrors.Should().BeTrue();
    }

    [Fact]
    public void HasErrors_ReturnsFalseWhenNoErrorsAndNoErrorMessage()
    {
        var tracker = new BatchMetricsTracker
        {
            FailedRecords = 0,
            ErrorMessage = null
        };

        tracker.HasErrors.Should().BeFalse();
    }

    [Fact]
    public void HasErrors_ReturnsFalseWhenErrorMessageIsEmpty()
    {
        var tracker = new BatchMetricsTracker
        {
            FailedRecords = 0,
            ErrorMessage = ""
        };

        tracker.HasErrors.Should().BeFalse();
    }
}

public class FileMetricsTrackerTests
{
    [Fact]
    public void DefaultValues_AreSetCorrectly()
    {
        var tracker = new FileMetricsTracker();

        tracker.FileName.Should().BeEmpty();
        tracker.StartTime.Should().Be(default);
        tracker.SizeBytes.Should().Be(0);
        tracker.RecordCount.Should().Be(0);
        tracker.RecordsProcessed.Should().Be(0);
        tracker.RecordsSkipped.Should().Be(0);
        tracker.RecordsCreated.Should().Be(0);
        tracker.RecordsUpdated.Should().Be(0);
        tracker.RecordsDeleted.Should().Be(0);
    }

    [Fact]
    public void AllProperties_CanBeSet()
    {
        var startTime = DateTime.UtcNow.AddMinutes(-10);

        var tracker = new FileMetricsTracker
        {
            FileName = "data_2024-06-15.csv",
            StartTime = startTime,
            SizeBytes = 1024000,
            RecordCount = 1000,
            RecordsProcessed = 950,
            RecordsSkipped = 50,
            RecordsCreated = 500,
            RecordsUpdated = 400,
            RecordsDeleted = 50
        };

        tracker.FileName.Should().Be("data_2024-06-15.csv");
        tracker.StartTime.Should().Be(startTime);
        tracker.SizeBytes.Should().Be(1024000);
        tracker.RecordCount.Should().Be(1000);
        tracker.RecordsProcessed.Should().Be(950);
        tracker.RecordsSkipped.Should().Be(50);
        tracker.RecordsCreated.Should().Be(500);
        tracker.RecordsUpdated.Should().Be(400);
        tracker.RecordsDeleted.Should().Be(50);
    }

    [Fact]
    public void ElapsedMilliseconds_CalculatesFromStartTime()
    {
        var tracker = new FileMetricsTracker
        {
            StartTime = DateTime.UtcNow.AddSeconds(-3)
        };

        tracker.ElapsedMilliseconds.Should().BeGreaterThanOrEqualTo(2900);
        tracker.ElapsedMilliseconds.Should().BeLessThan(4000);
    }

    [Fact]
    public void AddBatch_AccumulatesMetrics()
    {
        var tracker = new FileMetricsTracker
        {
            RecordsProcessed = 100,
            RecordsCreated = 50,
            RecordsUpdated = 30,
            RecordsDeleted = 20
        };

        var batchMetrics = new BatchProcessingMetrics
        {
            RecordsProcessed = 50,
            RecordsCreated = 25,
            RecordsUpdated = 15,
            RecordsDeleted = 10
        };

        tracker.AddBatch(batchMetrics);

        tracker.RecordsProcessed.Should().Be(150);
        tracker.RecordsCreated.Should().Be(75);
        tracker.RecordsUpdated.Should().Be(45);
        tracker.RecordsDeleted.Should().Be(30);
    }

    [Fact]
    public void AddBatch_MultipleBatches_AccumulatesCorrectly()
    {
        var tracker = new FileMetricsTracker();

        var batch1 = new BatchProcessingMetrics
        {
            RecordsProcessed = 100,
            RecordsCreated = 60,
            RecordsUpdated = 30,
            RecordsDeleted = 10
        };

        var batch2 = new BatchProcessingMetrics
        {
            RecordsProcessed = 100,
            RecordsCreated = 40,
            RecordsUpdated = 50,
            RecordsDeleted = 10
        };

        tracker.AddBatch(batch1);
        tracker.AddBatch(batch2);

        tracker.RecordsProcessed.Should().Be(200);
        tracker.RecordsCreated.Should().Be(100);
        tracker.RecordsUpdated.Should().Be(80);
        tracker.RecordsDeleted.Should().Be(20);
    }
}

public class BatchIngestionMetricsTests
{
    [Fact]
    public void Record_CanBeCreated()
    {
        var metrics = new BatchIngestionMetrics
        {
            RecordsProcessed = 100,
            RecordsCreated = 60,
            RecordsUpdated = 30,
            RecordsDeleted = 10
        };

        metrics.RecordsProcessed.Should().Be(100);
        metrics.RecordsCreated.Should().Be(60);
        metrics.RecordsUpdated.Should().Be(30);
        metrics.RecordsDeleted.Should().Be(10);
    }

    [Fact]
    public void Record_SupportsEquality()
    {
        var metrics1 = new BatchIngestionMetrics { RecordsProcessed = 100 };
        var metrics2 = new BatchIngestionMetrics { RecordsProcessed = 100 };
        var metrics3 = new BatchIngestionMetrics { RecordsProcessed = 200 };

        metrics1.Should().Be(metrics2);
        metrics1.Should().NotBe(metrics3);
    }
}

public class BatchProcessingMetricsTests
{
    [Fact]
    public void Record_CanBeCreated()
    {
        var metrics = new BatchProcessingMetrics
        {
            RecordsProcessed = 100,
            RecordsCreated = 60,
            RecordsUpdated = 30,
            RecordsDeleted = 10
        };

        metrics.RecordsProcessed.Should().Be(100);
        metrics.RecordsCreated.Should().Be(60);
        metrics.RecordsUpdated.Should().Be(30);
        metrics.RecordsDeleted.Should().Be(10);
    }

    [Fact]
    public void Record_SupportsWithExpression()
    {
        var original = new BatchProcessingMetrics { RecordsProcessed = 100 };
        var modified = original with { RecordsCreated = 50 };

        modified.RecordsProcessed.Should().Be(100);
        modified.RecordsCreated.Should().Be(50);
    }
}
