using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Snapshots;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;
using Microsoft.Extensions.Logging.Abstractions;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>Snapshot. Input: NormalisedFileSet. Output: SnapshotFile.
/// Every normalised file newer than the latest snapshot is folded onto it, oldest first, and the
/// result is written under the newest source timestamp applied.</summary>
public class SnapshotStageTests
{
    private const string Header = "CHANGE_TYPE|CPH|HOLDING_NAME";

    private static readonly DataSetDefinition SamCph = Definition("sam_cph_holdings");

    private readonly InMemoryEtlPipelineStorage _storage = new();

    private InMemoryBlobStorage Normalised => _storage.Folder(EtlPipelineFolders.Normalised);
    private InMemoryBlobStorage Snapshots => _storage.Folder(EtlPipelineFolders.Snapshots);

    private static DataSetDefinition Definition(string name, DataSetIngestionMode mode = DataSetIngestionMode.Delta) =>
        new(name, $"{name}_{{0}}", ["CPH"], ChangeType.HeaderName, [], IngestionMode: mode);

    private Task<List<SnapshotFile>> RunAsync(params NormalisedFileSet[] inputs) =>
        StageRunner.RunAsync(
            new SnapshotStage(
                _storage,
                new ParquetDeltaMergeEngine(NullLogger<ParquetDeltaMergeEngine>.Instance),
                NullLogger<SnapshotStage>.Instance),
            inputs);

    private void PutNormalised(string dataSet, string timestamp, params string[] rows)
        => Normalised.Put($"{dataSet}/{dataSet}_{timestamp}.parquet", ParquetFixture.From(Header, rows));

    [Fact]
    public async Task Names_the_snapshot_after_the_newest_source_timestamp_applied_not_the_etl_run()
    {
        PutNormalised("sam_cph_holdings", "20251113121333", "I|01/001/0001|Old Farm");
        PutNormalised("sam_cph_holdings", "20251115121333", "U|01/001/0001|Updated Farm");

        var output = await RunAsync(new NormalisedFileSet(SamCph));

        output.Should().ContainSingle()
            .Which.Key.Should().Be("sam_cph_holdings/sam_cph_holdings_20251115121333.parquet");
        Snapshots.Keys.Should().ContainSingle();
    }

    [Fact]
    public async Task Folds_every_normalised_file_when_no_snapshot_exists_yet()
    {
        PutNormalised("sam_cph_holdings", "20251113121333", "I|01/001/0001|Old Farm", "I|01/001/0002|Keep Farm");
        PutNormalised("sam_cph_holdings", "20251114121333", "U|01/001/0001|Updated Farm", "I|01/001/0003|New Farm");
        PutNormalised("sam_cph_holdings", "20251115121333", "D|01/001/0002|Should Not Delete");

        var output = await RunAsync(new NormalisedFileSet(SamCph));

        ParquetFixture.ToLines(Snapshots.BytesOf(output.Single().Key)).Should().Equal(
            "CPH|HOLDING_NAME",
            "01/001/0001|Updated Farm",
            "01/001/0002|Keep Farm",
            "01/001/0003|New Farm");
    }

    [Fact]
    public async Task Applies_only_the_files_newer_than_the_latest_snapshot()
    {
        PutNormalised("sam_cph_holdings", "20251113121333", "I|01/001/0001|Old Farm");

        var first = await RunAsync(new NormalisedFileSet(SamCph));

        PutNormalised("sam_cph_holdings", "20251114121333", "U|01/001/0001|Updated Farm");

        var second = await RunAsync(new NormalisedFileSet(SamCph));

        first.Single().AppliedKeys.Should().Equal("sam_cph_holdings/sam_cph_holdings_20251113121333.parquet");
        second.Single().AppliedKeys.Should().Equal("sam_cph_holdings/sam_cph_holdings_20251114121333.parquet");
        ParquetFixture.ToLines(Snapshots.BytesOf(second.Single().Key)).Should().Equal(
            "CPH|HOLDING_NAME",
            "01/001/0001|Updated Farm");
    }

    [Fact]
    public async Task Drops_the_change_type_column_from_the_snapshot()
    {
        PutNormalised("sam_cph_holdings", "20251113121333", "I|01/001/0001|Old Farm");

        var output = await RunAsync(new NormalisedFileSet(SamCph));

        ParquetFixture.ToLines(Snapshots.BytesOf(output.Single().Key))[0].Should().Be("CPH|HOLDING_NAME");
    }

    [Fact]
    public async Task Reuses_the_existing_snapshot_when_nothing_newer_has_arrived()
    {
        PutNormalised("sam_cph_holdings", "20251113121333", "I|01/001/0001|Old Farm");

        var first = await RunAsync(new NormalisedFileSet(SamCph));
        var second = await RunAsync(new NormalisedFileSet(SamCph));

        second.Single().Key.Should().Be(first.Single().Key);
        second.Single().Created.Should().BeFalse();
        second.Single().AppliedKeys.Should().BeEmpty();
        Snapshots.Keys.Should().ContainSingle();
    }

    [Fact]
    public async Task Retains_older_snapshots_rather_than_replacing_them()
    {
        PutNormalised("sam_cph_holdings", "20251113121333", "I|01/001/0001|Old Farm");
        await RunAsync(new NormalisedFileSet(SamCph));

        PutNormalised("sam_cph_holdings", "20251114121333", "U|01/001/0001|Updated Farm");
        await RunAsync(new NormalisedFileSet(SamCph));

        Snapshots.Keys.Should().BeEquivalentTo(
            "sam_cph_holdings/sam_cph_holdings_20251113121333.parquet",
            "sam_cph_holdings/sam_cph_holdings_20251114121333.parquet");
    }

    [Fact]
    public async Task Never_overwrites_an_existing_snapshot_file()
    {
        var key = "sam_cph_holdings/sam_cph_holdings_20251113121333.parquet";
        PutNormalised("sam_cph_holdings", "20251113121333", "I|01/001/0001|Old Farm");
        Snapshots.Put(key, "written by someone else");

        var output = await RunAsync(new NormalisedFileSet(SamCph));

        output.Single().Created.Should().BeFalse();
        Snapshots.ContentOf(key).Should().Be("written by someone else");
    }

    [Fact]
    public async Task Reports_what_the_merge_did()
    {
        PutNormalised("sam_cph_holdings", "20251113121333", "I|01/001/0001|Old Farm", "I|01/001/0002|Keep Farm");
        PutNormalised("sam_cph_holdings", "20251115121333", "D|01/001/0002|Should Not Delete");

        var output = await RunAsync(new NormalisedFileSet(SamCph));

        output.Single().Should().BeEquivalentTo(new
        {
            SourceTimestamp = new DateTimeOffset(2025, 11, 15, 12, 13, 33, TimeSpan.Zero),
            Created = true,
            RowCount = 2L,
            RowsUpserted = 2L,
            RowsIgnoredDeletes = 1L
        });
    }

    [Fact]
    public async Task Fails_the_import_when_a_normalised_file_carries_no_source_timestamp()
    {
        Normalised.Put("sam_cph_holdings/sam_cph_holdings.parquet", ParquetFixture.From(Header, "I|01/001/0001|Old Farm"));

        var run = async () => await RunAsync(new NormalisedFileSet(SamCph));

        await run.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*sam_cph_holdings.parquet*");
    }

    [Fact]
    public async Task Fails_the_import_when_two_normalised_files_share_a_source_timestamp()
    {
        Normalised.Put("sam_cph_holdings/sam_cph_holdings_20251113121333.parquet", ParquetFixture.From(Header, "I|01/001/0001|Old Farm"));
        Normalised.Put("sam_cph_holdings/other_20251113121333.parquet", ParquetFixture.From(Header, "I|01/001/0002|Keep Farm"));

        var run = async () => await RunAsync(new NormalisedFileSet(SamCph));

        await run.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*no rule for which to apply first*");
    }

    [Fact]
    public async Task Prefers_the_files_carried_by_the_payload_over_listing_the_folder()
    {
        PutNormalised("sam_cph_holdings", "20251113121333", "I|01/001/0001|Old Farm");
        PutNormalised("sam_cph_holdings", "20251114121333", "U|01/001/0001|Updated Farm");

        var output = await RunAsync(new NormalisedFileSet(SamCph)
        {
            Files = ["sam_cph_holdings/sam_cph_holdings_20251113121333.parquet"]
        });

        output.Single().Key.Should().Be("sam_cph_holdings/sam_cph_holdings_20251113121333.parquet");
    }

    [Fact]
    public async Task Produces_nothing_when_the_dataset_has_no_normalised_file()
    {
        var output = await RunAsync(new NormalisedFileSet(SamCph));

        output.Should().BeEmpty();
        Snapshots.Keys.Should().BeEmpty();
    }

    [Fact]
    public async Task Produces_nothing_for_an_empty_input()
    {
        var output = await RunAsync();

        output.Should().BeEmpty();
    }

    [Fact]
    public async Task Produces_one_snapshot_per_dataset()
    {
        PutNormalised("sam_cph_holdings", "20251113121333", "I|01/001/0001|Old Farm");
        PutNormalised("cts_keeper", "20251113121333", "I|02/002/0002|Other Farm");

        var output = await RunAsync(
            new NormalisedFileSet(SamCph),
            new NormalisedFileSet(Definition("cts_keeper")));

        output.Select(o => o.Definition.Name).Should().Equal("sam_cph_holdings", "cts_keeper");
        Snapshots.Keys.Should().HaveCount(2);
    }

    [Fact]
    public async Task A_snapshot_mode_dataset_copies_its_latest_normalised_file_unchanged()
    {
        var definition = Definition("sam_showground", DataSetIngestionMode.Snapshot);
        Normalised.Put("sam_showground/sam_showground_20251113121333.parquet", "older");
        Normalised.Put("sam_showground/sam_showground_20251115121333.parquet", "latest");

        var output = await RunAsync(new NormalisedFileSet(definition));

        output.Single().Key.Should().Be("sam_showground/sam_showground_20251115121333.parquet");
        Snapshots.ContentOf(output.Single().Key).Should().Be("latest");
    }
}
