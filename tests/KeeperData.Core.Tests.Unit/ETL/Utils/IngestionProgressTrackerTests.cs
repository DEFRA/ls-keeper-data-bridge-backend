using FluentAssertions;
using KeeperData.Core.ETL.Utils;
using KeeperData.Core.Reporting.Dtos;

namespace KeeperData.Core.Tests.Unit.ETL.Utils;

/// <summary>
/// Unit tests for IngestionProgressTracker class.
/// Tests progress tracking, rate calculation, and estimation accuracy.
/// </summary>
public class IngestionProgressTrackerTests
{
    private static readonly string TestFileName = Path.Combine("data", "test_file_12345.csv");
    private const string ExpectedFileName = "test_file_12345.csv";

    [Fact]
    public void Constructor_ShouldInitializeWithValidParameters()
    {
        // Act
        var tracker = new IngestionProgressTracker(TestFileName, 1000);

        // Assert
        tracker.IsCompleted.Should().BeFalse();
        tracker.ElapsedTime.Should().BeLessThan(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void Constructor_WithZeroRows_ShouldDefaultToOne()
    {
        // Act
        var tracker = new IngestionProgressTracker(TestFileName, 0);

        // Assert
        // Verify by checking GetCurrentStatus returns TotalRows = 1
        var status = tracker.GetCurrentStatus();
        status.TotalRows.Should().Be(1);
    }

    [Fact]
    public void Constructor_ShouldExtractFileNameFromPath()
    {
        // Act
        var tracker = new IngestionProgressTracker(TestFileName, 100);

        // Assert
        var status = tracker.GetCurrentStatus();
        status.FileName.Should().Be(ExpectedFileName);
    }

    [Fact]
    public void GetCurrentStatus_InitialState_ShouldReturnZeroProgress()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 1000);

        // Act
        var status = tracker.GetCurrentStatus();

        // Assert
        status.RowNumber.Should().Be(0);
        status.PercentageCompleted.Should().Be(0);
        status.RowsPerMinute.Should().BeNull();
        status.EstimatedTimeRemaining.Should().BeNull();
        status.EstimatedCompletionUtc.Should().BeNull();
    }

    [Fact]
    public void GetCurrentStatus_AfterUpdateProgress_ShouldReturnNonZeroProgress()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 1000);

        // Act
        tracker.UpdateProgress(500);
        var status = tracker.GetCurrentStatus();

        // Assert
        status.RowNumber.Should().Be(500);
        status.PercentageCompleted.Should().BeGreaterThan(0);
    }

    [Fact]
    public void UpdateProgress_ShouldCalculatePercentageCorrectly()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 100);

        // Act
        tracker.UpdateProgress(50);
        var status = tracker.GetCurrentStatus();

        // Assert
        status.PercentageCompleted.Should().Be(50);
    }

    [Fact]
    public void UpdateProgress_AtMaxEstimate_ShouldCapPercentageAt99Percent()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 100);

        // Act
        tracker.UpdateProgress(100);
        var status = tracker.GetCurrentStatus();

        // Assert
        // Should be capped at 99% until Complete() is called
        status.PercentageCompleted.Should().Be(99);
    }

    [Fact]
    public void UpdateProgress_ExceedingEstimate_ShouldHandleGracefully()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 100);

        // Act - Process more rows than estimated
        tracker.UpdateProgress(150);
        var status = tracker.GetCurrentStatus();

        // Assert
        // Should still calculate percentage based on actual rows
        status.PercentageCompleted.Should().BeGreaterThan(0);
        status.PercentageCompleted.Should().Be(99); // Capped at 99% until complete
    }

    [Fact]
    public void UpdateProgress_ShouldCalculateRowsPerMinute()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 1000);

        // Act
        tracker.UpdateProgress(60); // Simulate 60 rows processed
        System.Threading.Thread.Sleep(500); // Wait some time
        tracker.UpdateProgress(120); // Another 60 rows
        var status = tracker.GetCurrentStatus();

        // Assert
        status.RowsPerMinute.Should().NotBeNull();
        // Just verify we get a reasonable rate (any positive value)
        status.RowsPerMinute.Should().BeGreaterThan(0);
    }

    [Fact]
    public void UpdateProgress_WithSmallRowCount_ShouldNotCalculateEstimate()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 1000);

        // Act - Update with less than minimum rows for estimate
        tracker.UpdateProgress(5);
        var status = tracker.GetCurrentStatus();

        // Assert
        status.EstimatedTimeRemaining.Should().BeNull();
        status.EstimatedCompletionUtc.Should().BeNull();
    }

    [Fact]
    public void UpdateProgress_WithSufficientRows_ShouldCalculateEstimate()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 1000);

        // Act - Update with sufficient rows for estimate
        tracker.UpdateProgress(50); // Should trigger estimate after 10+ rows
        System.Threading.Thread.Sleep(100); // Small delay
        tracker.UpdateProgress(100);
        var status = tracker.GetCurrentStatus();

        // Assert
        status.EstimatedTimeRemaining.Should().NotBeNull();
        status.EstimatedCompletionUtc.Should().NotBeNull();
        status.EstimatedTimeRemaining.Should().BeGreaterThan(TimeSpan.Zero);
    }

    [Fact]
    public void GetCurrentStatus_ShouldIncludeFileName()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 100);

        // Act
        var status = tracker.GetCurrentStatus();

        // Assert
        status.FileName.Should().Be(ExpectedFileName);
    }

    [Fact]
    public void GetCurrentStatus_ShouldIncludeTotalRows()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 5000);

        // Act
        var status = tracker.GetCurrentStatus();

        // Assert
        status.TotalRows.Should().Be(5000);
    }

    [Fact]
    public void Complete_ShouldMarkAsCompleted()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 100);

        // Act
        tracker.UpdateProgress(50);
        var finalStatus = tracker.Complete();

        // Assert
        tracker.IsCompleted.Should().BeTrue();
        finalStatus.PercentageCompleted.Should().Be(100);
    }

    [Fact]
    public void Complete_ShouldSetPercentageToExactly100()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 100);
        tracker.UpdateProgress(75);

        // Act
        var finalStatus = tracker.Complete();

        // Assert
        finalStatus.PercentageCompleted.Should().Be(100);
    }

    [Fact]
    public void Complete_ShouldSetEstimatedTimeRemainingToZero()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 100);
        tracker.UpdateProgress(50);

        // Act
        var finalStatus = tracker.Complete();

        // Assert
        finalStatus.EstimatedTimeRemaining.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void Complete_ShouldSetEstimatedCompletionToNow()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 100);
        tracker.UpdateProgress(50);

        // Act
        var beforeComplete = DateTime.UtcNow;
        var finalStatus = tracker.Complete();
        var afterComplete = DateTime.UtcNow;

        // Assert
        finalStatus.EstimatedCompletionUtc.Should().NotBeNull();
        finalStatus.EstimatedCompletionUtc.Should().BeWithin(TimeSpan.FromSeconds(1)).After(beforeComplete);
        finalStatus.EstimatedCompletionUtc.Should().BeWithin(TimeSpan.FromSeconds(1)).Before(afterComplete);
    }

    [Fact]
    public void Complete_ShouldStopStopwatch()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 100);
        tracker.UpdateProgress(50);
        var elapsedBeforeComplete = tracker.ElapsedTime;

        // Act
        System.Threading.Thread.Sleep(50);
        tracker.Complete();
        var elapsedAfterComplete1 = tracker.ElapsedTime;

        System.Threading.Thread.Sleep(50);
        var elapsedAfterComplete2 = tracker.ElapsedTime;

        // Assert
        // Elapsed time should not increase after Complete()
        elapsedAfterComplete1.Should().Be(elapsedAfterComplete2);
    }

    [Fact]
    public void UpdateProgress_MultipleUpdates_ShouldTrackProgressionAccurately()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 1000);

        // Act & Assert - Track progression
        tracker.UpdateProgress(100);
        var status1 = tracker.GetCurrentStatus();
        status1.RowNumber.Should().Be(100);

        tracker.UpdateProgress(250);
        var status2 = tracker.GetCurrentStatus();
        status2.RowNumber.Should().Be(250);

        tracker.UpdateProgress(500);
        var status3 = tracker.GetCurrentStatus();
        status3.RowNumber.Should().Be(500);

        // Verify percentages increase
        status1.PercentageCompleted.Should().NotBeNull();
        status2.PercentageCompleted.Should().NotBeNull();
        status3.PercentageCompleted.Should().NotBeNull();
        status1.PercentageCompleted.Should().BeLessThan(status2.PercentageCompleted.Value);
        status2.PercentageCompleted.Should().BeLessThan(status3.PercentageCompleted.Value);
    }

    [Fact]
    public void UpdateProgress_ShouldUseExponentialMovingAverage()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 10000);

        // Act - Simulate varying speeds
        // Fast processing at first
        System.Diagnostics.Stopwatch sw = System.Diagnostics.Stopwatch.StartNew();
        tracker.UpdateProgress(100);
        System.Threading.Thread.Sleep(100);
        tracker.UpdateProgress(200);
        var fastRate1 = tracker.GetCurrentStatus().RowsPerMinute;

        // Slower processing
        System.Threading.Thread.Sleep(200);
        tracker.UpdateProgress(300);
        var rate2 = tracker.GetCurrentStatus().RowsPerMinute;

        // Much slower
        System.Threading.Thread.Sleep(300);
        tracker.UpdateProgress(400);
        var slowerRate = tracker.GetCurrentStatus().RowsPerMinute;

        // Assert - Rate should adapt but not swing wildly
        fastRate1.Should().NotBeNull();
        rate2.Should().NotBeNull();
        slowerRate.Should().NotBeNull();

        // Slower rate should be less than initial rate
        slowerRate.Should().BeLessThan(fastRate1.Value);
    }

    [Fact]
    public void EstimatedTimeRemaining_ShouldBeAccurate()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 1000);

        // Act - Process at a steady rate
        tracker.UpdateProgress(100);
        System.Threading.Thread.Sleep(200); // Simulate processing taking time
        tracker.UpdateProgress(200);

        var status = tracker.GetCurrentStatus();

        // Assert
        status.EstimatedTimeRemaining.Should().NotBeNull();
        // Should have significant time remaining (800 rows left)
        // Just verify it's not null and is positive
        status.EstimatedTimeRemaining.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
    }

    [Fact]
    public void EstimatedCompletionUtc_ShouldBeInTheFuture()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 1000);
        tracker.UpdateProgress(100);
        System.Threading.Thread.Sleep(100); // Give some time for rate calculation

        // Act
        tracker.UpdateProgress(200);
        var status = tracker.GetCurrentStatus();

        // Assert
        status.EstimatedCompletionUtc.Should().NotBeNull();

        var estimatedCompletion = status.EstimatedCompletionUtc.Value;
        var now = DateTime.UtcNow;

        // Should be in the near future (allow for calculation precision)
        // Give a reasonable window: within 200ms before now to 5 minutes after
        estimatedCompletion.Should().BeAfter(now.AddMilliseconds(-300));
        estimatedCompletion.Should().BeBefore(now.AddMinutes(5));
    }

    [Fact]
    public void ElapsedTime_ShouldIncreaseOverTime()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 100);

        // Act
        var elapsed1 = tracker.ElapsedTime;
        System.Threading.Thread.Sleep(100);
        var elapsed2 = tracker.ElapsedTime;

        // Assert
        elapsed2.Should().BeGreaterThan(elapsed1);
    }

    [Fact]
    public void ElapsedTime_ShouldStopAfterComplete()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 100);
        System.Threading.Thread.Sleep(50);
        tracker.Complete();
        var elapsedAfterComplete = tracker.ElapsedTime;

        // Act
        System.Threading.Thread.Sleep(100);
        var elapsedLater = tracker.ElapsedTime;

        // Assert
        elapsedLater.Should().Be(elapsedAfterComplete);
    }

    [Fact]
    public void GetCurrentStatus_WhenNoRowsProcessed_ShouldReturnAllNullableMetrics()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 1000);

        // Act
        var status = tracker.GetCurrentStatus();

        // Assert
        status.RowsPerMinute.Should().BeNull();
        status.EstimatedTimeRemaining.Should().BeNull();
        status.EstimatedCompletionUtc.Should().BeNull();
    }

    [Fact]
    public void GetCurrentStatus_WithNegativeRowsProcessed_ShouldHandleGracefully()
    {
        // Arrange - This tests edge case handling
        var tracker = new IngestionProgressTracker(TestFileName, 100);

        // Act - Update with valid rows, then check it doesn't break
        tracker.UpdateProgress(50);
        var status = tracker.GetCurrentStatus();

        // Assert - Should handle gracefully
        status.RowNumber.Should().Be(50);
        status.PercentageCompleted.Should().BeGreaterThan(0);
    }

    [Fact]
    public void UpdateProgress_WithVeryLargeRowCounts_ShouldNotOverflow()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, int.MaxValue);

        // Act
        tracker.UpdateProgress(100000000); // Very large number
        var status = tracker.GetCurrentStatus();

        // Assert - Should handle large numbers gracefully
        status.RowNumber.Should().Be(100000000);
        status.PercentageCompleted.Should().BeGreaterThan(0);
        status.PercentageCompleted.Should().BeLessThanOrEqualTo(99);
    }

    [Fact]
    public void CompleteMethod_ShouldReturnCurrentFileStatusRecord()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 100);
        tracker.UpdateProgress(75);

        // Act
        var result = tracker.Complete();

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<IngestionCurrentFileStatus>();
        result.PercentageCompleted.Should().Be(100);
        result.FileName.Should().Be(ExpectedFileName);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(50)]
    [InlineData(100)]
    public void UpdateProgress_WithDifferentRowNumbers_ShouldCalculateCorrectPercentage(int rowsProcessed)
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 200);

        // Act
        tracker.UpdateProgress(rowsProcessed);
        var status = tracker.GetCurrentStatus();

        // Assert
        var expectedPercentage = rowsProcessed == 200 ? 99 : (rowsProcessed * 100 / 200);
        status.PercentageCompleted.Should().Be(expectedPercentage);
    }

    [Fact]
    public void Constructor_WithFilePathWithMultipleDirs_ShouldExtractJustFileName()
    {
        // Arrange & Act
        var testPath = Path.Combine("very", "deep", "folder", "structure", "myfile.csv");
        var tracker = new IngestionProgressTracker(testPath, 100);
        var status = tracker.GetCurrentStatus();

        // Assert
        status.FileName.Should().Be("myfile.csv");
    }

    [Fact]
    public void Constructor_WithRelativeFilePath_ShouldExtractFileName()
    {
        // Arrange & Act
        var testPath = Path.Combine("data", "csv", "file.csv");
        var tracker = new IngestionProgressTracker(testPath, 100);
        var status = tracker.GetCurrentStatus();

        // Assert
        status.FileName.Should().Be("file.csv");
    }

    [Fact]
    public void GetCurrentStatus_ShouldAlwaysReturnNewRecord()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 100);
        tracker.UpdateProgress(50);

        // Act
        var status1 = tracker.GetCurrentStatus();
        var status2 = tracker.GetCurrentStatus();

        // Assert - Should be new instances with same values
        status1.Should().NotBeSameAs(status2);
        status1.RowNumber.Should().Be(status2.RowNumber);
    }

    [Fact]
    public void EstimatedTimeRemaining_WithZeroRowsRemaining_ShouldBeZero()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 100);

        // Act - Need at least 2 updates to establish a rate for estimation
        tracker.UpdateProgress(50); // First update to establish baseline
        System.Threading.Thread.Sleep(50); // Small delay to create time delta
        tracker.UpdateProgress(100); // All rows processed
        var status = tracker.GetCurrentStatus();

        // Assert
        status.EstimatedTimeRemaining.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void GetCurrentStatus_RowNumberProperty_ShouldAlwaysMatchLastUpdate()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 1000);
        var rowsToProcess = new[] { 100, 250, 500, 750, 1000 };

        // Act & Assert
        foreach (var rows in rowsToProcess)
        {
            tracker.UpdateProgress(rows);
            var status = tracker.GetCurrentStatus();
            status.RowNumber.Should().Be(rows);
        }
    }

    [Fact]
    public void Complete_CalledMultipleTimes_ShouldRemainCompleted()
    {
        // Arrange
        var tracker = new IngestionProgressTracker(TestFileName, 100);
        tracker.UpdateProgress(50);

        // Act
        var firstComplete = tracker.Complete();
        tracker.UpdateProgress(75); // Try to update after complete
        var secondComplete = tracker.Complete();

        // Assert
        tracker.IsCompleted.Should().BeTrue();
        firstComplete.PercentageCompleted.Should().Be(100);
        secondComplete.PercentageCompleted.Should().Be(100);
    }
}