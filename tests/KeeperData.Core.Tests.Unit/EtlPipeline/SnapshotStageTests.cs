using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>Snapshot. Input: NormalisedFileSet. Output: SnapshotFile.
/// Snapshot mode only: the latest normalised parquet is copied to snapshots/ under the dataset's
/// clean name and a fresh ETL timestamp.</summary>
public class SnapshotStageTests
{
    private static readonly DataSetDefinition SamCph = StageRunner.Definition("sam_cph_holdings");

    private readonly InMemoryEtlPipelineStorage _storage = new();
    private readonly FakeTimeProvider _timeProvider = new(new DateTimeOffset(2026, 07, 28, 11, 22, 33, TimeSpan.Zero));

    private InMemoryBlobStorage Normalised => _storage.Folder(EtlPipelineFolders.Normalised);
    private InMemoryBlobStorage Snapshots => _storage.Folder(EtlPipelineFolders.Snapshots);

    private Task<List<SnapshotFile>> RunAsync(params NormalisedFileSet[] inputs) =>
        StageRunner.RunAsync(new SnapshotStage(_storage, _timeProvider, NullLogger<SnapshotStage>.Instance), inputs);

    [Fact]
    public async Task Writes_a_snapshot_named_after_the_dataset_and_the_etl_timestamp()
    {
        Normalised.Put("sam_cph_holdings/sam_cph_holdings_20260701000000.parquet", "rows");

        var output = await RunAsync(new NormalisedFileSet(SamCph));

        output.Should().ContainSingle()
            .Which.Key.Should().Be("sam_cph_holdings/sam_cph_holdings_20260728112233.parquet");
        Snapshots.ContentOf("sam_cph_holdings/sam_cph_holdings_20260728112233.parquet").Should().Be("rows");
    }

    [Fact]
    public async Task Snapshots_the_latest_normalised_file_by_filename_timestamp()
    {
        Normalised.Put("sam_cph_holdings/sam_cph_holdings_20260701000000.parquet", "old");
        Normalised.Put("sam_cph_holdings/sam_cph_holdings_20260715000000.parquet", "new");
        Normalised.Put("sam_cph_holdings/sam_cph_holdings_20260710000000.parquet", "middle");

        var output = await RunAsync(new NormalisedFileSet(SamCph));

        output.Single().SourceKey.Should().Be("sam_cph_holdings/sam_cph_holdings_20260715000000.parquet");
        Snapshots.ContentOf(output.Single().Key).Should().Be("new");
    }

    [Fact]
    public async Task Prefers_the_files_carried_by_the_payload_over_listing_the_folder()
    {
        Normalised.Put("sam_cph_holdings/sam_cph_holdings_20260701000000.parquet", "listed");
        Normalised.Put("sam_cph_holdings/sam_cph_holdings_20260715000000.parquet", "supplied");

        var output = await RunAsync(new NormalisedFileSet(SamCph)
        {
            Files = ["sam_cph_holdings/sam_cph_holdings_20260701000000.parquet"]
        });

        output.Single().SourceKey.Should().Be("sam_cph_holdings/sam_cph_holdings_20260701000000.parquet");
    }

    [Fact]
    public async Task Does_not_create_a_duplicate_snapshot_when_there_is_no_new_normalised_file()
    {
        Normalised.Put("sam_cph_holdings/sam_cph_holdings_20260701000000.parquet", "rows");

        var first = await RunAsync(new NormalisedFileSet(SamCph));

        _timeProvider.Advance(TimeSpan.FromHours(1));

        var second = await RunAsync(new NormalisedFileSet(SamCph));

        second.Single().Key.Should().Be(first.Single().Key);
        second.Single().Created.Should().BeFalse();
        Snapshots.Keys.Should().ContainSingle();
    }

    [Fact]
    public async Task Creates_a_new_snapshot_when_a_newer_normalised_file_arrives()
    {
        Normalised.Put("sam_cph_holdings/sam_cph_holdings_20260701000000.parquet", "rows");
        await RunAsync(new NormalisedFileSet(SamCph));

        Normalised.Put("sam_cph_holdings/sam_cph_holdings_20260702000000.parquet", "more rows");
        _timeProvider.Advance(TimeSpan.FromHours(1));

        var output = await RunAsync(new NormalisedFileSet(SamCph));

        output.Single().Created.Should().BeTrue();
        Snapshots.Keys.Should().HaveCount(2);
        Snapshots.ContentOf(output.Single().Key).Should().Be("more rows");
    }

    [Fact]
    public async Task Never_overwrites_an_existing_snapshot_file()
    {
        var key = "sam_cph_holdings/sam_cph_holdings_20260728112233.parquet";
        Normalised.Put("sam_cph_holdings/sam_cph_holdings_20260701000000.parquet", "rows");
        Snapshots.Put(key, "written by someone else");

        var output = await RunAsync(new NormalisedFileSet(SamCph));

        output.Should().BeEmpty();
        Snapshots.ContentOf(key).Should().Be("written by someone else");
    }

    [Fact]
    public async Task Records_the_normalised_file_the_snapshot_came_from()
    {
        Normalised.Put("sam_cph_holdings/sam_cph_holdings_20260701000000.parquet", "rows");

        var output = await RunAsync(new NormalisedFileSet(SamCph));

        Snapshots.MetadataOf(output.Single().Key)[EtlConstants.MetadataKeySnapshotSourceKey]
            .Should().Be("sam_cph_holdings/sam_cph_holdings_20260701000000.parquet");
    }

    [Fact]
    public async Task Produces_nothing_when_the_dataset_has_no_normalised_file()
    {
        var output = await RunAsync(new NormalisedFileSet(SamCph));

        output.Should().BeEmpty();
        Snapshots.Keys.Should().BeEmpty();
    }

    [Fact]
    public async Task Ignores_normalised_files_whose_name_carries_no_timestamp()
    {
        Normalised.Put("sam_cph_holdings/sam_cph_holdings.parquet", "unnamed");
        Normalised.Put("sam_cph_holdings/sam_cph_holdings_20260701000000.parquet", "rows");

        var output = await RunAsync(new NormalisedFileSet(SamCph));

        output.Single().SourceKey.Should().Be("sam_cph_holdings/sam_cph_holdings_20260701000000.parquet");
    }

    [Fact]
    public async Task Produces_one_snapshot_per_dataset()
    {
        Normalised.Put("sam_cph_holdings/sam_cph_holdings_20260701000000.parquet", "sam");
        Normalised.Put("cts_keeper/cts_keeper_20260701000000.parquet", "cts");

        var output = await RunAsync(
            new NormalisedFileSet(SamCph),
            new NormalisedFileSet(StageRunner.Definition("cts_keeper")));

        output.Select(o => o.Definition.Name).Should().Equal("sam_cph_holdings", "cts_keeper");
        Snapshots.Keys.Should().HaveCount(2);
    }

    [Fact]
    public async Task Produces_nothing_for_an_empty_input()
    {
        var output = await RunAsync();

        output.Should().BeEmpty();
    }

    [Fact]
    public async Task The_latest_snapshot_is_the_one_with_the_newest_etl_timestamp()
    {
        Normalised.Put("sam_cph_holdings/sam_cph_holdings_20260701000000.parquet", "first");
        var first = await RunAsync(new NormalisedFileSet(SamCph));

        Normalised.Put("sam_cph_holdings/sam_cph_holdings_20260702000000.parquet", "second");
        _timeProvider.Advance(TimeSpan.FromHours(1));
        var second = await RunAsync(new NormalisedFileSet(SamCph));

        Normalised.Put("sam_cph_holdings/sam_cph_holdings_20260703000000.parquet", "third");
        _timeProvider.Advance(TimeSpan.FromHours(1));
        var third = await RunAsync(new NormalisedFileSet(SamCph));

        var allKeys = Snapshots.Keys.ToList();
        allKeys.Should().HaveCount(3);

        var latest = SnapshotFileNaming.LatestByTimestamp(SamCph, allKeys);

        latest.Should().Be(third.Single().Key);
    }
}
