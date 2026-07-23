using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Infrastructure.Storage;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Core.ETL;

/// <summary>
/// Integration tests for <see cref="BulkListingExternalCatalogueService"/> against LocalStack.
///
/// The ticket's acceptance criterion is that discovery results are unchanged, so the decisive test
/// here is parity: the same queries are run through both catalogues over the same real S3 data and
/// must return identical file sets in identical order.
///
/// Uses its own top-level folder so it cannot collide with the legacy suite's fixtures.
/// </summary>
[Collection("LocalStack"), Trait("Dependence", "docker")]
public class BulkListingExternalCatalogueServiceIntegrationTests : IAsyncLifetime
{
    private const string TestTopLevelFolder = "litprd-bulk";

    private static readonly DateOnly RangeStart = new(2024, 10, 1);
    private static readonly DateOnly RangeEnd = new(2024, 10, 31);
    private static readonly DateOnly BeforeRange = new(2024, 9, 15);
    private static readonly DateOnly AfterRange = new(2024, 11, 5);

    private readonly ITestOutputHelper _testOutputHelper;
    private readonly LocalStackFixture _localStackFixture;
    private readonly TestDataSetDefinitions _definitions;
    private readonly IExternalCatalogueService _bulkListingCatalogue;
    private readonly IExternalCatalogueService _legacyCatalogue;
    private readonly List<string> _createdTestFileKeys = [];

    public BulkListingExternalCatalogueServiceIntegrationTests(ITestOutputHelper testOutputHelper, LocalStackFixture localStackFixture)
    {
        _testOutputHelper = testOutputHelper;
        _localStackFixture = localStackFixture;

        var blobService = new S3BlobStorageServiceReadOnly(
            _localStackFixture.S3Client,
            new Mock<ILogger<S3BlobStorageServiceReadOnly>>().Object,
            LocalStackFixture.TestBucket,
            TestTopLevelFolder);

        _definitions = new TestDataSetDefinitions();
        var timeProvider = new FakeTimeProvider(new DateTimeOffset(2024, 12, 15, 10, 0, 0, TimeSpan.Zero));

        _bulkListingCatalogue = new BulkListingExternalCatalogueService(blobService, timeProvider, _definitions);
        _legacyCatalogue = new LegacyExternalCatalogueService(blobService, timeProvider, _definitions);
    }

    public Task InitializeAsync() => SetupTestDataAsync();

    public Task DisposeAsync() => CleanupTestDataAsync();

    [Fact]
    public async Task GetFileSetAsync_ForASingleDate_ReturnsThatDaysFile()
    {
        var date = new DateOnly(2024, 10, 15);

        var fileSet = await _bulkListingCatalogue.GetFileSetAsync(_definitions.SamCPHHolding, date, CancellationToken.None);

        fileSet.Definition.Should().Be(_definitions.SamCPHHolding);
        fileSet.Files.Should().ContainSingle();
        fileSet.Files[0].StorageObject.Key.Should().Contain("LITP_SAMCPHHOLDING_20241015120000");
        fileSet.Files[0].StorageObject.Container.Should().Be(LocalStackFixture.TestBucket);
    }

    [Fact]
    public async Task GetFileSetAsync_ForARange_ExcludesFilesOutsideItAndOrdersAscending()
    {
        var fileSet = await _bulkListingCatalogue.GetFileSetAsync(_definitions.SamCPHHolding, RangeStart, RangeEnd, CancellationToken.None);

        var dates = fileSet.Files.Select(file => DateOnly.FromDateTime(file.Timestamp.UtcDateTime)).ToArray();

        dates.Should().NotContain(BeforeRange).And.NotContain(AfterRange);
        dates.Should().OnlyContain(date => date >= RangeStart && date <= RangeEnd);
        fileSet.Files.Select(file => file.Timestamp).Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task GetFileSetsAsync_ForAllDefinitions_ReturnsOneFileSetPerDefinition()
    {
        var fileSets = await _bulkListingCatalogue.GetFileSetsAsync(_definitions.All, RangeStart, RangeEnd, CancellationToken.None);

        fileSets.Should().HaveCount(_definitions.All.Length);
        fileSets.Select(set => set.Definition).Should().Equal(_definitions.All);
        fileSets.Should().OnlyContain(set => set.Files.Length > 0);
    }

    [Fact]
    public async Task GetFileSetsAsync_ForARange_DiscoversExactlyWhatTheLegacyCatalogueDoes()
    {
        var bulkResults = await _bulkListingCatalogue.GetFileSetsAsync(_definitions.All, RangeStart, RangeEnd, CancellationToken.None);
        var legacyResults = await _legacyCatalogue.GetFileSetsAsync(_definitions.All, RangeStart, RangeEnd, CancellationToken.None);

        AssertSameDiscovery(bulkResults, legacyResults);
    }

    [Fact]
    public async Task GetFileSetsAsync_ForALookbackWindow_DiscoversExactlyWhatTheLegacyCatalogueDoes()
    {
        const int lookbackDays = 90;

        var bulkResults = await _bulkListingCatalogue.GetFileSetsAsync(lookbackDays, CancellationToken.None);
        var legacyResults = await _legacyCatalogue.GetFileSetsAsync(lookbackDays, CancellationToken.None);

        AssertSameDiscovery(bulkResults, legacyResults);
    }

    private void AssertSameDiscovery(IEnumerable<FileSet> bulkResults, IEnumerable<FileSet> legacyResults)
    {
        foreach (var definition in _definitions.All)
        {
            var bulkKeys = KeysFor(bulkResults, definition);
            var legacyKeys = KeysFor(legacyResults, definition);

            _testOutputHelper.WriteLine($"{definition.Name}: bulk {bulkKeys.Length} file(s), legacy {legacyKeys.Length} file(s)");

            bulkKeys.Should().Equal(legacyKeys,
                "both catalogues should discover the same files, in the same order, for {0}", definition.Name);
        }
    }

    private static string[] KeysFor(IEnumerable<FileSet> fileSets, DataSetDefinition definition)
        => [.. fileSets.Single(set => set.Definition == definition).Files.Select(file => file.StorageObject.Key)];

    private async Task SetupTestDataAsync()
    {
        var dates = DatesToCreate().ToArray();

        foreach (var date in dates)
        {
            foreach (var definition in _definitions.All)
            {
                var key = $"{TestTopLevelFolder}/{FileNameFor(definition, date)}";

                await _localStackFixture.S3Client.PutObjectAsync(new Amazon.S3.Model.PutObjectRequest
                {
                    BucketName = LocalStackFixture.TestBucket,
                    Key = key,
                    ContentBody = " ",
                    ContentType = "text/plain"
                });

                _createdTestFileKeys.Add(key);
            }
        }

        _testOutputHelper.WriteLine($"Created {_createdTestFileKeys.Count} test file(s) under {TestTopLevelFolder}");
    }

    private static IEnumerable<DateOnly> DatesToCreate()
    {
        yield return BeforeRange;

        for (var date = RangeStart; date <= RangeEnd; date = date.AddDays(1))
        {
            yield return date;
        }

        yield return AfterRange;
    }

    private static string FileNameFor(DataSetDefinition definition, DateOnly date)
        => string.Format(definition.FilePrefixFormat, date.ToString("yyyyMMdd") + "120000") + ".csv";

    private async Task CleanupTestDataAsync()
    {
        foreach (var key in _createdTestFileKeys)
        {
            try
            {
                await _localStackFixture.S3Client.DeleteObjectAsync(new Amazon.S3.Model.DeleteObjectRequest
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
}
