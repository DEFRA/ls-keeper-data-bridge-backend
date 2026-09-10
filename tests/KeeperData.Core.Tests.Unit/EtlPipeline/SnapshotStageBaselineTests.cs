using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Snapshots;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;
using Microsoft.Extensions.Logging.Abstractions;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>Snapshot, for a dataset whose files arrive as a baseline plus deltas. The set of bulk file
/// names decides whether a run folds deltas onto the existing snapshot or rebuilds from the bulk files
/// and replays only the deltas cut after them.</summary>
public class SnapshotStageBaselineTests
{
    private const string Header = "CHANGE_TYPE|CPH|HOLDING_NAME";

    private const string Dataset = "cts_location_identifiers";

    private static readonly DataSetDefinition Cts = new(
        Dataset,
        "cads/cts/",
        ["CPH"],
        ChangeType.HeaderName,
        [],
        DateTimePattern: "yyyy-MM-dd-HHmmss",
        IngestionMode: DataSetIngestionMode.Delta,
        SourceKeyPattern: "cads/cts/{bulk,daily}/*CT_LOCATION_IDENTIFIERS*",
        BaselineKeyPattern: "cads/cts/bulk/*_BULK_*CT_LOCATION_IDENTIFIERS*");

    private readonly InMemoryEtlPipelineStorage _storage = new();

    private InMemoryBlobStorage Normalised => _storage.Folder(EtlPipelineFolders.Normalised);
    private InMemoryBlobStorage Snapshots => _storage.Folder(EtlPipelineFolders.Snapshots);

    private Task<List<SnapshotFile>> RunAsync() =>
        StageRunner.RunAsync(
            new SnapshotStage(
                _storage,
                new ParquetDeltaMergeEngine(NullLogger<ParquetDeltaMergeEngine>.Instance),
                NullLogger<SnapshotStage>.Instance),
            new NormalisedFileSet[] { new(Cts) });

    private string PutBulk(string part, string timestamp, params string[] rows)
        => PutBulkRun("00001", part, timestamp, rows);

    private string PutBulkRun(string run, string part, string timestamp, params string[] rows)
        => Put($"CTSM_CADS_PROD_BULK_{run}_{part}_CT_LOCATION_IDENTIFIERS_{timestamp}", rows);

    private string PutDelta(string run, string timestamp, params string[] rows)
        => Put($"CTSM_CADS_PROD_DELTA_{run}_001_CT_LOCATION_IDENTIFIERS_{timestamp}", rows);

    private string Put(string fileName, string[] rows)
    {
        var key = $"{Dataset}/{fileName}.parquet";

        Normalised.Put(key, ParquetFixture.From(Header, rows));

        return key;
    }

    [Fact]
    public async Task Names_the_snapshot_after_the_bulk_set_it_was_built_from()
    {
        PutBulk("001", "2026-08-22-072824", "I|01/001/0001|Old Farm");

        var output = await RunAsync();

        SnapshotFileNaming.TryExtractBaselineHash(output.Single().Key, out var hash).Should().BeTrue();
        output.Single().Key.Should().Be($"{Dataset}/{Dataset}_b{hash}_2026-08-22-072824.parquet");
    }

    [Fact]
    public async Task Names_a_baseline_with_no_deltas_yet_after_the_newest_bulk()
    {
        PutBulk("001", "2026-08-22-072824", "I|01/001/0001|Old Farm");
        PutBulk("002", "2026-08-22-072826", "I|01/001/0002|Keep Farm");

        var output = await RunAsync();

        output.Single().Key.Should().EndWith("_2026-08-22-072826.parquet");
        output.Single().SourceTimestamp.Should().Be(new DateTimeOffset(2026, 08, 22, 07, 28, 26, TimeSpan.Zero));
    }

    /// <summary>Parts of one extract share a timestamp, so bulk files must never meet the duplicate
    /// check that keeps the delta sequence honest.</summary>
    [Fact]
    public async Task Accepts_two_bulk_parts_sharing_a_timestamp()
    {
        PutBulk("001", "2026-08-22-072824", "I|01/001/0001|Old Farm");
        PutBulk("002", "2026-08-22-072824", "I|01/001/0002|Keep Farm");

        var output = await RunAsync();

        ParquetFixture.ToLines(Snapshots.BytesOf(output.Single().Key)).Should().Equal(
            "CPH|HOLDING_NAME",
            "01/001/0001|Old Farm",
            "01/001/0002|Keep Farm");
    }

    [Fact]
    public async Task Applies_only_the_deltas_newer_than_the_snapshot_while_the_bulk_set_stands()
    {
        PutBulk("001", "2026-08-22-072824", "I|01/001/0001|Old Farm");
        PutDelta("00002", "2026-08-23-063010", "U|01/001/0001|Updated Farm");

        var first = await RunAsync();

        var newest = PutDelta("00003", "2026-08-24-063014", "I|01/001/0002|New Farm");

        var second = await RunAsync();

        first.Single().SourceTimestamp.Should().Be(new DateTimeOffset(2026, 08, 23, 06, 30, 10, TimeSpan.Zero));
        second.Single().AppliedKeys.Should().Equal(newest);
        second.Single().Key.Should().EndWith("_2026-08-24-063014.parquet");
    }

    [Fact]
    public async Task Does_nothing_on_a_re_run_with_no_new_delta()
    {
        PutBulk("001", "2026-08-22-072824", "I|01/001/0001|Old Farm");
        PutDelta("00002", "2026-08-23-063010", "U|01/001/0001|Updated Farm");

        var first = await RunAsync();
        var second = await RunAsync();

        second.Single().Key.Should().Be(first.Single().Key);
        second.Single().Created.Should().BeFalse();
        Snapshots.Keys.Should().ContainSingle();
    }

    /// <summary>A new bulk part with no new delta resolves to the timestamp the previous snapshot
    /// already carries. The hash is what keeps the two lineages apart, so the rebuild is written rather
    /// than declining to overwrite and silently keeping stale data.</summary>
    [Fact]
    public async Task Rebuilds_under_a_new_key_when_a_bulk_part_appears_at_the_same_timestamp()
    {
        PutBulk("001", "2026-08-22-072824", "I|01/001/0001|Old Farm");

        var first = await RunAsync();

        PutBulk("002", "2026-08-22-072824", "I|01/001/0002|Keep Farm");

        var second = await RunAsync();

        second.Single().Key.Should().NotBe(first.Single().Key);
        second.Single().Created.Should().BeTrue();
        second.Single().Key.Should().EndWith("_2026-08-22-072824.parquet");
        Snapshots.Keys.Should().HaveCount(2);
        ParquetFixture.ToLines(Snapshots.BytesOf(second.Single().Key)).Should().Equal(
            "CPH|HOLDING_NAME",
            "01/001/0001|Old Farm",
            "01/001/0002|Keep Farm");
    }

    /// <summary>Replaying a delta cut before the new baseline would revert the rows the baseline
    /// already carries, because the merge is last-writer-wins.</summary>
    [Fact]
    public async Task Does_not_replay_the_deltas_older_than_a_new_bulk()
    {
        var firstBulk = PutBulk("001", "2026-08-22-072824", "I|01/001/0001|Old Farm");
        var stale = PutDelta("00002", "2026-08-23-063010", "U|01/001/0001|Stale Farm");

        await RunAsync();

        var secondBulk = PutBulk("002", "2026-08-25-072824", "I|01/001/0001|Rebaselined Farm");
        var newest = PutDelta("00003", "2026-08-26-063014", "I|01/001/0002|New Farm");

        var rebuilt = await RunAsync();

        rebuilt.Single().AppliedKeys.Should().Equal(firstBulk, secondBulk, newest);
        rebuilt.Single().AppliedKeys.Should().NotContain(stale);
        ParquetFixture.ToLines(Snapshots.BytesOf(rebuilt.Single().Key)).Should().Equal(
            "CPH|HOLDING_NAME",
            "01/001/0001|Rebaselined Farm",
            "01/001/0002|New Farm");
    }

    /// <summary>A lane holds every bulk run the feed has ever cut, and each run is a complete extract in
    /// its own right. Folding an earlier run in with the newest would restate the rows the newest has
    /// since dropped, because a row deleted between the two runs is simply absent from the newer files
    /// and absence is not a delete.</summary>
    [Fact]
    public async Task Baselines_on_the_newest_bulk_run_alone()
    {
        PutBulkRun("00001", "001", "2026-08-01-072824", "I|01/001/0001|Retired Farm", "I|01/001/0002|Old Farm");
        var newest = PutBulkRun("00005", "001", "2026-08-22-072824", "I|01/001/0002|Kept Farm");

        var output = await RunAsync();

        output.Single().AppliedKeys.Should().Equal(newest);
        ParquetFixture.ToLines(Snapshots.BytesOf(output.Single().Key)).Should().Equal(
            "CPH|HOLDING_NAME",
            "01/001/0002|Kept Farm");
    }

    /// <summary>The parts of a run are disjoint cuts of one extract, so all of them are the baseline -
    /// keeping only the newest part would drop most of the dataset for a table cut into eleven.</summary>
    [Fact]
    public async Task Baselines_on_every_part_of_the_newest_bulk_run()
    {
        PutBulkRun("00001", "001", "2026-08-01-072824", "I|01/001/0009|Retired Farm");
        var first = PutBulkRun("00005", "001", "2026-08-22-072824", "I|01/001/0001|One Farm");
        var second = PutBulkRun("00005", "002", "2026-08-22-072830", "I|01/001/0002|Two Farm");
        var delta = PutDelta("00006", "2026-08-23-063010", "I|01/001/0003|Three Farm");

        var output = await RunAsync();

        output.Single().AppliedKeys.Should().Equal(first, second, delta);
        ParquetFixture.ToLines(Snapshots.BytesOf(output.Single().Key)).Should().Equal(
            "CPH|HOLDING_NAME",
            "01/001/0001|One Farm",
            "01/001/0002|Two Farm",
            "01/001/0003|Three Farm");
    }

    /// <summary>Two of the five sample delta files are header-only; without the name moving on they
    /// would be reprocessed on every run forever.</summary>
    [Fact]
    public async Task Advances_the_name_on_a_delta_carrying_no_rows()
    {
        PutBulk("001", "2026-08-22-072824", "I|01/001/0001|Old Farm");

        await RunAsync();

        PutDelta("00002", "2026-08-23-063010");

        var second = await RunAsync();

        second.Single().Key.Should().EndWith("_2026-08-23-063010.parquet");
        second.Single().Created.Should().BeTrue();

        var third = await RunAsync();

        third.Single().Created.Should().BeFalse();
    }
}
