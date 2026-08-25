using System.Collections.Immutable;
using FluentAssertions;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using Microsoft.Extensions.Time.Testing;
using Moq;

namespace KeeperData.Core.Tests.Unit.ETL;

public class BulkListingExternalCatalogueServiceTests
{
    private static readonly DataSetDefinition DataSetA = new("dataset_a", "LITP_A_{0}", ["KEY"], "change_type", []);
    private static readonly DataSetDefinition DataSetB = new("dataset_b", "LITP_B_{0}", ["KEY"], "change_type", []);
    private static readonly DataSetDefinition DataSetC = new("dataset_c", "LITP_C_{0}", ["KEY"], "change_type", []);

    private static readonly DateOnly RangeStart = new(2024, 10, 1);
    private static readonly DateOnly RangeEnd = new(2024, 10, 31);
    private const int DaysInRange = 31;

    private readonly Mock<IBlobStorageServiceReadOnly> _blobs = new();
    private readonly BulkListingExternalCatalogueService _catalogue;

    public BulkListingExternalCatalogueServiceTests()
        => _catalogue = new BulkListingExternalCatalogueService(_blobs.Object, TimeProvider.System, Mock.Of<IDataSetDefinitions>());

    [Fact]
    public async Task ListsOncePerDataSet_RegardlessOfHowLongTheRangeIs()
    {
        var definitions = ImmutableArray.Create(DataSetA, DataSetB, DataSetC);
        GivenStoredFiles(
            FileFor(DataSetA, new DateOnly(2024, 10, 5)),
            FileFor(DataSetB, new DateOnly(2024, 10, 10)),
            FileFor(DataSetC, new DateOnly(2024, 10, 20)));

        await _catalogue.GetFileSetsAsync(definitions, RangeStart, RangeEnd, CancellationToken.None);

        _blobs.Verify(
            b => b.ListAsync(It.IsAny<string?>(), It.IsAny<CancellationToken>()),
            Times.Exactly(definitions.Length));
    }

    [Fact]
    public async Task ListsUnderTheDateIndependentDataSetPrefix()
    {
        GivenStoredFiles(FileFor(DataSetA, new DateOnly(2024, 10, 5)));

        await _catalogue.GetFileSetAsync(DataSetA, RangeStart, RangeEnd, CancellationToken.None);

        _blobs.Verify(b => b.ListAsync("LITP_A_", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ReturnsOnlyFilesWithinTheRange_IncludingBothBoundaries()
    {
        GivenStoredFiles(
            FileFor(DataSetA, new DateOnly(2024, 9, 30)),
            FileFor(DataSetA, RangeStart),
            FileFor(DataSetA, new DateOnly(2024, 10, 15)),
            FileFor(DataSetA, RangeEnd),
            FileFor(DataSetA, new DateOnly(2024, 11, 1)));

        var fileSets = await _catalogue.GetFileSetsAsync([DataSetA], RangeStart, RangeEnd, CancellationToken.None);

        DatesOf(fileSets.Single()).Should().Equal(RangeStart, new DateOnly(2024, 10, 15), RangeEnd);
    }

    [Fact]
    public async Task ReturnsOneFileSetPerRequestedDataSet_IncludingThoseWithNoFiles()
    {
        var definitions = ImmutableArray.Create(DataSetA, DataSetB, DataSetC);
        GivenStoredFiles(
            FileFor(DataSetA, new DateOnly(2024, 10, 5)),
            FileFor(DataSetC, new DateOnly(2024, 10, 6)));

        var fileSets = await _catalogue.GetFileSetsAsync(definitions, RangeStart, RangeEnd, CancellationToken.None);

        fileSets.Should().HaveCount(3);
        fileSets.Single(set => set.Definition == DataSetB).Files.Should().BeEmpty();
    }

    [Fact]
    public async Task OrdersFilesByTimestampAscending()
    {
        GivenStoredFiles(
            FileFor(DataSetA, new DateOnly(2024, 10, 20)),
            FileFor(DataSetA, new DateOnly(2024, 10, 3)),
            FileFor(DataSetA, new DateOnly(2024, 10, 11)));

        var fileSets = await _catalogue.GetFileSetsAsync([DataSetA], RangeStart, RangeEnd, CancellationToken.None);

        fileSets.Single().Files.Select(file => file.Timestamp).Should().BeInAscendingOrder();
    }

    [Fact]
    public async Task PreservesTheOrderOfTheRequestedDataSets()
    {
        var definitions = ImmutableArray.Create(DataSetC, DataSetA, DataSetB);
        GivenStoredFiles(
            FileFor(DataSetA, new DateOnly(2024, 10, 5)),
            FileFor(DataSetB, new DateOnly(2024, 10, 6)),
            FileFor(DataSetC, new DateOnly(2024, 10, 7)));

        var fileSets = await _catalogue.GetFileSetsAsync(definitions, RangeStart, RangeEnd, CancellationToken.None);

        fileSets.Select(set => set.Definition).Should().Equal(DataSetC, DataSetA, DataSetB);
    }

    [Fact]
    public async Task ASingleDateReturnsOnlyThatDay()
    {
        var wanted = new DateOnly(2024, 10, 15);
        GivenStoredFiles(
            FileFor(DataSetA, wanted.AddDays(-1)),
            FileFor(DataSetA, wanted),
            FileFor(DataSetA, wanted.AddDays(1)));

        var fileSet = await _catalogue.GetFileSetAsync(DataSetA, wanted, CancellationToken.None);

        DatesOf(fileSet).Should().Equal(wanted);
    }

    [Fact]
    public async Task ForALookbackWindow_CoversTheLastNDaysIncludingToday()
    {
        var today = new DateOnly(2024, 10, 20);
        var catalogue = CatalogueAt(today);
        GivenStoredFiles(
            FileFor(DataSetA, today.AddDays(-10)),
            FileFor(DataSetA, today.AddDays(-5)),
            FileFor(DataSetA, today.AddDays(-2)),
            FileFor(DataSetA, today));

        var fileSets = await catalogue.GetFileSetsAsync(days: 7, CancellationToken.None);

        DatesOf(fileSets.Single()).Should().Equal(today.AddDays(-5), today.AddDays(-2), today);
    }

    [Fact]
    public async Task ForZeroDays_ReturnsOnlyTodaysFiles()
    {
        var today = new DateOnly(2024, 10, 20);
        var catalogue = CatalogueAt(today);
        GivenStoredFiles(
            FileFor(DataSetA, today.AddDays(-1)),
            FileFor(DataSetA, today),
            FileFor(DataSetA, today.AddDays(1)));

        var fileSets = await catalogue.GetFileSetsAsync(days: 0, CancellationToken.None);

        DatesOf(fileSets.Single()).Should().Equal(today);
    }

    [Fact]
    public async Task WithNoArgumentsBeyondCancellation_ReturnsOnlyTodaysFiles()
    {
        var today = new DateOnly(2024, 10, 20);
        var catalogue = CatalogueAt(today);
        GivenStoredFiles(
            FileFor(DataSetA, today.AddDays(-1)),
            FileFor(DataSetA, today));

        var fileSets = await catalogue.GetFileSetsAsync(CancellationToken.None);

        DatesOf(fileSets.Single()).Should().Equal(today);
    }

    [Fact]
    public async Task ForASingleDate_AcrossTheConfiguredDataSets_ReturnsThatDayOnly()
    {
        var today = new DateOnly(2024, 10, 20);
        var catalogue = CatalogueAt(today);
        GivenStoredFiles(
            FileFor(DataSetA, today.AddDays(-1)),
            FileFor(DataSetA, today));

        var fileSets = await catalogue.GetFileSetsAsync(today, CancellationToken.None);

        DatesOf(fileSets.Single()).Should().Equal(today);
    }

    [Fact]
    public async Task ForADateRange_AcrossTheConfiguredDataSets_ReturnsTheWholeRange()
    {
        var today = new DateOnly(2024, 10, 20);
        var catalogue = CatalogueAt(today);
        GivenStoredFiles(
            FileFor(DataSetA, new DateOnly(2024, 10, 2)),
            FileFor(DataSetA, new DateOnly(2024, 10, 18)),
            FileFor(DataSetA, new DateOnly(2024, 11, 3)));

        var fileSets = await catalogue.GetFileSetsAsync(RangeStart, RangeEnd, CancellationToken.None);

        DatesOf(fileSets.Single()).Should().Equal(new DateOnly(2024, 10, 2), new DateOnly(2024, 10, 18));
    }

    [Fact]
    public async Task ForASingleDefinitionAndDateAcrossOverloads_ReturnsThatDayOnly()
    {
        var definitions = ImmutableArray.Create(DataSetA, DataSetB);
        var date = new DateOnly(2024, 10, 15);
        GivenStoredFiles(
            FileFor(DataSetA, date),
            FileFor(DataSetB, date),
            FileFor(DataSetA, date.AddDays(-1)));

        var fileSets = await _catalogue.GetFileSetsAsync(definitions, date, CancellationToken.None);

        fileSets.Should().HaveCount(2);
        fileSets.Should().OnlyContain(set => set.Files.Length == 1);
    }

    /// <summary>A catalogue whose clock reads <paramref name="today"/> and whose only dataset is A.</summary>
    private BulkListingExternalCatalogueService CatalogueAt(DateOnly today)
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(today.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero));

        var definitions = new Mock<IDataSetDefinitions>();
        definitions.Setup(d => d.All).Returns([DataSetA]);

        return new BulkListingExternalCatalogueService(_blobs.Object, clock, definitions.Object);
    }

    [Fact]
    public async Task DiscoversTheSameFilesAsTheLegacyCatalogue()
    {
        var definitions = ImmutableArray.Create(DataSetA, DataSetB);
        var stored = new[]
        {
            FileFor(DataSetA, new DateOnly(2024, 10, 20)),
            FileFor(DataSetA, new DateOnly(2024, 10, 3)),
            FileFor(DataSetB, new DateOnly(2024, 10, 11)),
            FileFor(DataSetA, new DateOnly(2024, 9, 15)),
            FileFor(DataSetB, new DateOnly(2024, 11, 2))
        };

        GivenStoredFiles(stored);
        var bulkResults = await _catalogue.GetFileSetsAsync(definitions, RangeStart, RangeEnd, CancellationToken.None);

        var legacyResults = await CreateLegacyOver(stored)
            .GetFileSetsAsync(definitions, RangeStart, RangeEnd, CancellationToken.None);

        foreach (var definition in definitions)
        {
            KeysFor(bulkResults, definition)
                .Should().Equal(KeysFor(legacyResults, definition),
                    "both catalogues should discover the same files, in the same order, for {0}", definition.Name);
        }
    }

    [Fact]
    public async Task CostsFarFewerListingsThanTheLegacyCatalogue()
    {
        var definitions = ImmutableArray.Create(DataSetA, DataSetB, DataSetC);
        var stored = new[] { FileFor(DataSetA, new DateOnly(2024, 10, 5)) };

        GivenStoredFiles(stored);
        await _catalogue.GetFileSetsAsync(definitions, RangeStart, RangeEnd, CancellationToken.None);

        var legacyBlobs = new Mock<IBlobStorageServiceReadOnly>();
        StubListing(legacyBlobs, stored);
        await new LegacyExternalCatalogueService(legacyBlobs.Object, TimeProvider.System, Mock.Of<IDataSetDefinitions>())
            .GetFileSetsAsync(definitions, RangeStart, RangeEnd, CancellationToken.None);

        _blobs.Verify(b => b.ListAsync(It.IsAny<string?>(), It.IsAny<CancellationToken>()),
            Times.Exactly(definitions.Length));
        legacyBlobs.Verify(b => b.ListAsync(It.IsAny<string?>(), It.IsAny<CancellationToken>()),
            Times.Exactly(DaysInRange * definitions.Length));
    }

    private static IExternalCatalogueService CreateLegacyOver(StorageObjectInfo[] stored)
    {
        var legacyBlobs = new Mock<IBlobStorageServiceReadOnly>();
        StubListing(legacyBlobs, stored);

        return new LegacyExternalCatalogueService(legacyBlobs.Object, TimeProvider.System, Mock.Of<IDataSetDefinitions>());
    }

    private void GivenStoredFiles(params StorageObjectInfo[] stored) => StubListing(_blobs, stored);

    /// <summary>Stubs listing the way prefix-based storage behaves: keys starting with the requested prefix.</summary>
    private static void StubListing(Mock<IBlobStorageServiceReadOnly> blobs, StorageObjectInfo[] stored)
        => blobs
            .Setup(b => b.ListAsync(It.IsAny<string?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string? prefix, CancellationToken _) =>
                stored.Where(file => file.Key.StartsWith(prefix ?? string.Empty, StringComparison.Ordinal)).ToList());

    private static StorageObjectInfo FileFor(DataSetDefinition definition, DateOnly date)
    {
        var key = string.Format(definition.FilePrefixFormat, date.ToString("yyyyMMdd") + "120000") + ".csv";

        return new StorageObjectInfo
        {
            Container = "test-bucket",
            Key = key,
            Size = 1,
            LastModified = date.ToDateTime(new TimeOnly(12, 0), DateTimeKind.Utc),
            ETag = "etag",
            StorageUri = new Uri($"s3://test-bucket/{key}")
        };
    }

    private static IEnumerable<DateOnly> DatesOf(FileSet fileSet)
        => fileSet.Files.Select(file => DateOnly.FromDateTime(file.Timestamp.UtcDateTime));

    private static string[] KeysFor(IEnumerable<FileSet> fileSets, DataSetDefinition definition)
        => [.. fileSets.Single(set => set.Definition == definition).Files.Select(file => file.StorageObject.Key)];
}
