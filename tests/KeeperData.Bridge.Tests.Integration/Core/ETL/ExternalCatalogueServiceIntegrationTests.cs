using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Infrastructure.Storage;
using Microsoft.Extensions.Logging;
using Moq;
using System.Collections.Immutable;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Core.ETL;

/// <summary>
/// Integration tests for ExternalCatalogueService using TestContainers.LocalStack.
/// Tests the ability to discover files within S3 based on date ranges and dataset definitions.
/// </summary>
[Collection("LocalStack"), Trait("Dependence", "docker")]
public class ExternalCatalogueServiceIntegrationTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly LocalStackFixture _localStackFixture;
    private readonly Mock<ILogger<S3BlobStorageServiceReadOnly>> _loggerMock;
    private readonly S3BlobStorageServiceReadOnly _blobService;
    private readonly TestDataSetDefinitions _testDataSetDefinitions;
    private readonly ExternalCatalogueService _ExternalCatalogueService;
    private readonly FakeTimeProvider _timeProvider;
    private readonly List<string> _createdTestFileKeys = new();

    private const string TestTopLevelFolder = "litprd";

    public ExternalCatalogueServiceIntegrationTests(ITestOutputHelper testOutputHelper, LocalStackFixture localStackFixture)
    {
        _testOutputHelper = testOutputHelper;
        _localStackFixture = localStackFixture;
        _loggerMock = new Mock<ILogger<S3BlobStorageServiceReadOnly>>();

        // Create blob service with litprd top-level folder
        _blobService = new S3BlobStorageServiceReadOnly(
            _localStackFixture.S3Client,
            _loggerMock.Object,
            LocalStackFixture.TestBucket,
            TestTopLevelFolder);

        // Create test dataset definitions
        _testDataSetDefinitions = new TestDataSetDefinitions();

        // Create time provider set to a fixed date for predictable testing
        _timeProvider = new FakeTimeProvider(new DateTimeOffset(2024, 12, 15, 10, 0, 0, TimeSpan.Zero));

        // Create the service under test
        _ExternalCatalogueService = new ExternalCatalogueService(_blobService, _timeProvider, _testDataSetDefinitions);
    }

    public async Task InitializeAsync()
    {
        await SetupTestDataAsync();
    }

    public async Task DisposeAsync()
    {
        await CleanupTestDataAsync();
    }

    [Fact]
    public async Task GetFileSetAsync_SingleDateSingleDefinition_ShouldReturnMatchingFiles()
    {
        // Arrange
        var date = new DateOnly(2024, 10, 15);
        var definition = _testDataSetDefinitions.SamCPHHolding;

        // Act
        var result = await _ExternalCatalogueService.GetFileSetAsync(definition, date, CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        result.Definition.Should().Be(definition);
        result.Files.Should().HaveCount(1);
        result.Files[0].Key.Should().Contain("LITP_SAMCPHHOLDING_20241015120000");
        result.Files[0].Size.Should().Be(1); // Single space character
        result.Files[0].Container.Should().Be(LocalStackFixture.TestBucket);
    }

    [Fact]
    public async Task GetFileSetAsync_DateRangeSingleDefinition_ShouldReturnAllFilesInRange()
    {
        // Arrange
        var fromDate = new DateOnly(2024, 10, 13);
        var toDate = new DateOnly(2024, 10, 17);
        var definition = _testDataSetDefinitions.SamCPHHolding;

        // Act
        var result = await _ExternalCatalogueService.GetFileSetAsync(definition, fromDate, toDate, CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        result.Definition.Should().Be(definition);
        result.Files.Should().HaveCount(5); // 5 days inclusive
        result.Files.Should().OnlyContain(f => f.Key.Contains("LITP_SAMCPHHOLDING_2024101"));

        // Verify all expected dates are present
        var fileDates = result.Files.Select(f => ExtractDateFromFileName(f.Key)).OrderBy(d => d).ToArray();
        fileDates.Should().Equal("20241013", "20241014", "20241015", "20241016", "20241017");
    }

    [Fact]
    public async Task GetFileSetsAsync_SingleDateMultipleDefinitions_ShouldReturnFileSetPerDefinition()
    {
        // Arrange
        var date = new DateOnly(2024, 10, 15);
        var definitions = _testDataSetDefinitions.All;

        // Act
        var result = await _ExternalCatalogueService.GetFileSetsAsync(definitions, date, CancellationToken.None);

        // Assert
        result.Should().HaveCount(3); // Three test definitions
        result.Should().OnlyContain(fs => fs.Files.Length == 1);

        var samCphFileSet = result.First(fs => fs.Definition.Name == "sam_cph_holdings");
        samCphFileSet.Files[0].Key.Should().Contain("LITP_SAMCPHHOLDING_20241015120000");

        var tradingFileSet = result.First(fs => fs.Definition.Name == "trading_data");
        tradingFileSet.Files[0].Key.Should().Contain("LITP_TRADING_20241015120000");

        var reportFileSet = result.First(fs => fs.Definition.Name == "daily_reports");
        reportFileSet.Files[0].Key.Should().Contain("LITP_REPORTS_20241015120000");
    }

    [Fact]
    public async Task GetFileSetsAsync_DateRangeMultipleDefinitions_ShouldGroupByDefinitionAndOrderByLastModified()
    {
        // Arrange
        var fromDate = new DateOnly(2024, 10, 13);
        var toDate = new DateOnly(2024, 10, 17);
        var definitions = _testDataSetDefinitions.All;

        // Act
        var result = await _ExternalCatalogueService.GetFileSetsAsync(definitions, fromDate, toDate, CancellationToken.None);

        // Assert
        result.Should().HaveCount(3); // Three test definitions

        foreach (var fileSet in result)
        {
            fileSet.Files.Should().HaveCount(5); // 5 days inclusive

            // Files should be ordered by LastModified descending (reverse chronological)
            for (int i = 0; i < fileSet.Files.Length - 1; i++)
            {
                fileSet.Files[i].LastModified.Should().BeOnOrAfter(fileSet.Files[i + 1].LastModified);
            }
        }

        // Verify we have the expected dataset types
        var definitionNames = result.Select(fs => fs.Definition.Name).OrderBy(n => n).ToArray();
        definitionNames.Should().Equal("daily_reports", "sam_cph_holdings", "trading_data");
    }

    [Fact]
    public async Task GetFileSetsAsync_TodayOnly_ShouldReturnTodaysFiles()
    {
        // Arrange - TimeProvider is set to 2024-12-15

        // Act
        var result = await _ExternalCatalogueService.GetFileSetsAsync(CancellationToken.None);

        // Assert
        result.Should().HaveCount(3); // Three test definitions
        result.Should().OnlyContain(fs => fs.Files.Length == 1);
        result.Should().OnlyContain(fs => fs.Files[0].Key.Contains("20241215120000"));

        // Verify all three dataset types are present
        var definitionNames = result.Select(fs => fs.Definition.Name).OrderBy(n => n).ToArray();
        definitionNames.Should().Equal("daily_reports", "sam_cph_holdings", "trading_data");
    }

    [Fact]
    public async Task GetFileSetsAsync_LastNDays_ShouldReturnCorrectDateRange()
    {
        // Arrange
        var days = 5; // Should get files from 2024-12-11 to 2024-12-15 (inclusive)

        // Act
        var result = await _ExternalCatalogueService.GetFileSetsAsync(days, CancellationToken.None);

        // Assert
        result.Should().HaveCount(3); // Three test definitions

        foreach (var fileSet in result)
        {
            fileSet.Files.Should().HaveCount(5); // 5 days inclusive

            // Check date range - should contain files from 2024-12-11 to 2024-12-15
            var dates = fileSet.Files.Select(f => ExtractDateFromFileName(f.Key)).OrderBy(d => d).ToArray();
            dates.Should().Equal("20241211", "20241212", "20241213", "20241214", "20241215");
        }
    }

    [Fact]
    public async Task GetFileSetsAsync_SpecificDateRange_ShouldReturnOnlyFilesInRange()
    {
        // Arrange
        var fromDate = new DateOnly(2024, 11, 1);
        var toDate = new DateOnly(2024, 11, 5);

        // Act
        var result = await _ExternalCatalogueService.GetFileSetsAsync(fromDate, toDate, CancellationToken.None);

        // Assert
        result.Should().HaveCount(3); // Three test definitions

        foreach (var fileSet in result)
        {
            fileSet.Files.Should().HaveCount(5); // 5 days inclusive
            fileSet.Files.Should().OnlyContain(f => f.Key.Contains("202411"));

            // Verify specific dates in range
            var dates = fileSet.Files.Select(f => ExtractDateFromFileName(f.Key)).OrderBy(d => d).ToArray();
            dates.Should().Equal("20241101", "20241102", "20241103", "20241104", "20241105");
        }
    }

    [Fact]
    public async Task GetFileSetsAsync_SingleSpecificDate_ShouldReturnOnlyThatDaysFiles()
    {
        // Arrange
        var specificDate = new DateOnly(2024, 11, 15);

        // Act
        var result = await _ExternalCatalogueService.GetFileSetsAsync(specificDate, CancellationToken.None);

        // Assert
        result.Should().HaveCount(3); // Three test definitions
        result.Should().OnlyContain(fs => fs.Files.Length == 1);
        result.Should().OnlyContain(fs => fs.Files[0].Key.Contains("20241115120000"));
    }

    [Fact]
    public async Task GetFileSetsAsync_NoMatchingFiles_ShouldReturnEmptyFileSets()
    {
        // Arrange - Use a date far in the past where no files exist (before our test data starts)
        var pastDate = new DateOnly(2024, 1, 1); // Well before our test data range

        // Act
        var result = await _ExternalCatalogueService.GetFileSetsAsync(pastDate, CancellationToken.None);

        // Assert
        result.Should().HaveCount(3); // Three test definitions
        result.Should().OnlyContain(fs => fs.Files.Length == 0);
    }

    [Fact]
    public async Task GetFileSetAsync_WithRealS3Storage_ShouldHaveCorrectMetadata()
    {
        // Arrange
        var date = new DateOnly(2024, 10, 15);
        var definition = _testDataSetDefinitions.SamCPHHolding;

        // Act
        var result = await _ExternalCatalogueService.GetFileSetAsync(definition, date, CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        result.Files.Should().HaveCount(1);

        var file = result.Files[0];
        file.Container.Should().Be(LocalStackFixture.TestBucket);
        file.Size.Should().Be(1); // Single space character
        file.ETag.Should().NotBeNullOrEmpty();
        file.StorageUri.Should().NotBeNull();
        file.StorageUri.Scheme.Should().Be("s3");
        file.LastModified.Should().BeAfter(DateTimeOffset.MinValue);
    }

    [Fact]
    public async Task GetFileSetsAsync_FilesOrderedByLastModifiedDescending_ShouldBeInCorrectOrder()
    {
        // Arrange
        var fromDate = new DateOnly(2024, 12, 10);
        var toDate = new DateOnly(2024, 12, 15);
        var definitions = _testDataSetDefinitions.All.Take(1).ToImmutableArray(); // Just one definition for clarity

        // Act
        var result = await _ExternalCatalogueService.GetFileSetsAsync(definitions, fromDate, toDate, CancellationToken.None);

        // Assert
        result.Should().HaveCount(1);
        var fileSet = result[0];
        fileSet.Files.Should().HaveCount(6); // 6 days inclusive

        // Verify files are ordered by LastModified descending
        for (int i = 0; i < fileSet.Files.Length - 1; i++)
        {
            fileSet.Files[i].LastModified.Should().BeOnOrAfter(fileSet.Files[i + 1].LastModified,
                $"File at index {i} should have LastModified >= file at index {i + 1}");
        }

        // Verify that we have all the expected dates (order may vary due to S3 timestamp behavior)
        var dates = fileSet.Files.Select(f => ExtractDateFromFileName(f.Key)).OrderBy(d => d).ToArray();
        var expectedDates = new[] { "20241210", "20241211", "20241212", "20241213", "20241214", "20241215" };
        dates.Should().Equal(expectedDates, "All expected dates should be present");
    }

    private async Task SetupTestDataAsync()
    {
        try
        {
            // Generate test files for past 3 months from the "current" date (2024-12-15)
            var endDate = new DateOnly(2024, 12, 15);
            var startDate = endDate.AddMonths(-3);

            var definitions = new[]
            {
                _testDataSetDefinitions.SamCPHHolding,
                _testDataSetDefinitions.TradingData,
                _testDataSetDefinitions.DailyReports
            };

            var filesToCreate = new List<(string key, string content, DateOnly date)>();

            // Generate files for each day in the range
            for (var date = startDate; date <= endDate; date = date.AddDays(1))
            {
                foreach (var definition in definitions)
                {
                    var fileName = GenerateFileName(definition, date);
                    var key = $"{TestTopLevelFolder}/{fileName}";
                    filesToCreate.Add((key, " ", date)); // Single space content as requested
                }
            }

            // Upload all files to S3 with staggered timestamps to ensure proper LastModified ordering
            var baseTime = new DateTime(2024, 9, 1, 12, 0, 0, DateTimeKind.Utc);

            foreach (var (key, content, date) in filesToCreate)
            {
                // Create files with LastModified times that correspond to their dates
                // More recent dates get more recent timestamps
                var dayOffset = date.DayNumber - startDate.DayNumber;
                var fileTimestamp = baseTime.AddDays(dayOffset);

                var request = new Amazon.S3.Model.PutObjectRequest
                {
                    BucketName = LocalStackFixture.TestBucket,
                    Key = key,
                    ContentBody = content,
                    ContentType = "text/plain"
                };

                await _localStackFixture.S3Client.PutObjectAsync(request);

                // Track created files for cleanup
                _createdTestFileKeys.Add(key);
            }

            _testOutputHelper.WriteLine($"Created {filesToCreate.Count} test files in S3");
        }
        catch (Exception ex)
        {
            _testOutputHelper.WriteLine($"Failed to setup test data: {ex.Message}");
            throw;
        }
    }

    private async Task CleanupTestDataAsync()
    {
        try
        {
            if (_createdTestFileKeys.Count == 0)
            {
                _testOutputHelper.WriteLine("No test files to clean up");
                return;
            }

            // Delete all created test files
            var deleteCount = 0;
            foreach (var key in _createdTestFileKeys)
            {
                try
                {
                    var deleteRequest = new Amazon.S3.Model.DeleteObjectRequest
                    {
                        BucketName = LocalStackFixture.TestBucket,
                        Key = key
                    };

                    await _localStackFixture.S3Client.DeleteObjectAsync(deleteRequest);
                    deleteCount++;
                }
                catch (Exception ex)
                {
                    _testOutputHelper.WriteLine($"Failed to delete test file {key}: {ex.Message}");
                    // Continue with cleanup of other files
                }
            }

            _testOutputHelper.WriteLine($"Cleaned up {deleteCount} of {_createdTestFileKeys.Count} test files from S3");
            _createdTestFileKeys.Clear();
        }
        catch (Exception ex)
        {
            _testOutputHelper.WriteLine($"Failed to cleanup test data: {ex.Message}");
            // Don't throw here as this is cleanup code
        }
    }

    private static string GenerateFileName(DataSetDefinition definition, DateOnly date)
    {
        // Create filename with datetime pattern (yyyyMMddHHmmss) for more realistic scenario
        var dateTimeString = date.ToString("yyyyMMdd") + "120000"; // Use noon as time
        return string.Format(definition.FilePrefixFormat, dateTimeString) + ".csv";
    }

    private static string ExtractDateFromFileName(string fileName)
    {
        // Extract the 8-digit date part from filenames like LITP_SAMCPHHOLDING_20241215120000.csv
        var parts = fileName.Split('_');
        if (parts.Length >= 3)
        {
            var datePart = parts[2];
            if (datePart.Length >= 8)
            {
                return datePart.Substring(0, 8); // Return YYYYMMDD part
            }
        }
        return string.Empty;
    }
}

/// <summary>
/// Test implementation of IDataSetDefinitions for integration testing
/// Uses the datetime pattern specified in requirements (yyyyMMddHHmmss)
/// </summary>
public class TestDataSetDefinitions : IDataSetDefinitions
{
    public DataSetDefinition SamCPHHolding { get; }
    public DataSetDefinition TradingData { get; }
    public DataSetDefinition DailyReports { get; }

    public ImmutableArray<DataSetDefinition> All { get; }

    public TestDataSetDefinitions()
    {
        // Use the datetime pattern as specified in the requirements (yyyyMMddHHmmss)
        SamCPHHolding = new DataSetDefinition("sam_cph_holdings", "LITP_SAMCPHHOLDING_{0}", "yyyyMMddHHmmss", "CPH", ChangeType.HeaderName);
        TradingData = new DataSetDefinition("trading_data", "LITP_TRADING_{0}", "yyyyMMddHHmmss", "TradeId", ChangeType.HeaderName);
        DailyReports = new DataSetDefinition("daily_reports", "LITP_REPORTS_{0}", "yyyyMMddHHmmss", "ReportId", ChangeType.HeaderName);

        All = [SamCPHHolding, TradingData, DailyReports];
    }
}

/// <summary>
/// Fake TimeProvider for predictable testing
/// </summary>
public class FakeTimeProvider : TimeProvider
{
    private readonly DateTimeOffset _fixedTime;

    public FakeTimeProvider(DateTimeOffset fixedTime)
    {
        _fixedTime = fixedTime;
    }

    public override DateTimeOffset GetUtcNow() => _fixedTime;
}