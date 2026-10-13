using FluentAssertions;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

public class SnapshotFileNamingTests
{
    [Fact]
    public void SnapshotKey_uses_the_clean_dataset_name_and_the_source_timestamp()
    {
        var key = SnapshotFileNaming.SnapshotKey(
            StageRunner.Definition("sam_cph_holdings"),
            new DateTimeOffset(2026, 07, 28, 11, 22, 33, TimeSpan.Zero));

        key.Should().Be("sam_cph_holdings/sam_cph_holdings_20260728112233.parquet");
    }

    [Fact]
    public void SnapshotKey_uses_the_utc_form_of_the_timestamp()
    {
        var key = SnapshotFileNaming.SnapshotKey(
            StageRunner.Definition("sam_cph_holdings"),
            new DateTimeOffset(2026, 07, 28, 12, 22, 33, TimeSpan.FromHours(1)));

        key.Should().EndWith("_20260728112233.parquet");
    }

    [Fact]
    public void SnapshotKey_carries_the_baseline_hash_ahead_of_the_timestamp()
    {
        var key = SnapshotFileNaming.SnapshotKey(
            StageRunner.Definition("cts_location_identifiers"),
            new DateTimeOffset(2026, 07, 28, 11, 22, 33, TimeSpan.Zero),
            "1a2b3c4d");

        key.Should().Be("cts_location_identifiers/cts_location_identifiers_b1a2b3c4d_20260728112233.parquet");
    }

    [Fact]
    public void SnapshotKey_keeps_the_unhashed_shape_for_a_dataset_with_no_baseline_lane()
    {
        var key = SnapshotFileNaming.SnapshotKey(
            StageRunner.Definition("sam_cph_holdings"),
            new DateTimeOffset(2026, 07, 28, 11, 22, 33, TimeSpan.Zero),
            null);

        key.Should().Be("sam_cph_holdings/sam_cph_holdings_20260728112233.parquet");
    }

    [Fact]
    public void TryExtractBaselineHash_reads_back_the_hash_the_key_was_written_with()
    {
        var key = SnapshotFileNaming.SnapshotKey(
            StageRunner.Definition("cts_location_identifiers"),
            new DateTimeOffset(2026, 07, 28, 11, 22, 33, TimeSpan.Zero),
            "1a2b3c4d");

        SnapshotFileNaming.TryExtractBaselineHash(key, out var hash).Should().BeTrue();
        hash.Should().Be("1a2b3c4d");
    }

    [Fact]
    public void TryExtractBaselineHash_finds_nothing_in_an_unhashed_key()
    {
        SnapshotFileNaming.TryExtractBaselineHash(
            "sam_cph_holdings/sam_cph_holdings_20260728112233.parquet", out var hash).Should().BeFalse();

        hash.Should().BeNull();
    }

    [Fact]
    public void LatestForHash_ignores_the_lineage_of_a_different_baseline_set()
    {
        var latest = SnapshotFileNaming.LatestForHash(
            StageRunner.Definition("cts_location_identifiers"),
            [
                "cts_location_identifiers/cts_location_identifiers_b1a2b3c4d_20260728112233.parquet",
                "cts_location_identifiers/cts_location_identifiers_bdeadbeef_20260801000000.parquet"
            ],
            "1a2b3c4d");

        latest.Should().Be("cts_location_identifiers/cts_location_identifiers_b1a2b3c4d_20260728112233.parquet");
    }

    [Fact]
    public void LatestForHash_finds_nothing_when_no_snapshot_carries_the_current_baseline()
    {
        var latest = SnapshotFileNaming.LatestForHash(
            StageRunner.Definition("cts_location_identifiers"),
            ["cts_location_identifiers/cts_location_identifiers_bdeadbeef_20260801000000.parquet"],
            "1a2b3c4d");

        latest.Should().BeNull();
    }

    [Fact]
    public void LatestForHash_matches_the_unhashed_snapshots_of_a_dataset_with_no_baseline_lane()
    {
        var latest = SnapshotFileNaming.LatestForHash(
            StageRunner.Definition("sam_cph_holdings"),
            [
                "sam_cph_holdings/sam_cph_holdings_20260701000000.parquet",
                "sam_cph_holdings/sam_cph_holdings_20260715000000.parquet"
            ],
            null);

        latest.Should().Be("sam_cph_holdings/sam_cph_holdings_20260715000000.parquet");
    }

    [Fact]
    public void LatestByTimestamp_picks_the_newest_and_ignores_unparsable_keys()
    {
        var latest = SnapshotFileNaming.LatestByTimestamp(
            StageRunner.Definition("sam_cph_holdings"),
            [
                "sam_cph_holdings/sam_cph_holdings.parquet",
                "sam_cph_holdings/sam_cph_holdings_20260701000000.parquet",
                "sam_cph_holdings/sam_cph_holdings_20260715000000.parquet"
            ]);

        latest.Should().Be("sam_cph_holdings/sam_cph_holdings_20260715000000.parquet");
    }

    [Fact]
    public void LatestByTimestamp_returns_null_when_nothing_is_usable()
    {
        SnapshotFileNaming.LatestByTimestamp(StageRunner.Definition(), ["no-timestamp.parquet"]).Should().BeNull();
    }

    [Fact]
    public void OrderedByTimestamp_orders_oldest_first_by_the_timestamp_in_the_name()
    {
        var ordered = SnapshotFileNaming.OrderedByTimestamp(
            StageRunner.Definition("sam_cph_holdings"),
            [
                "sam_cph_holdings/sam_cph_holdings_20260715000000.parquet",
                "sam_cph_holdings/sam_cph_holdings_20260701000000.parquet",
                "sam_cph_holdings/sam_cph_holdings_20260710000000.parquet"
            ]);

        ordered.Select(item => item.Key).Should().Equal(
            "sam_cph_holdings/sam_cph_holdings_20260701000000.parquet",
            "sam_cph_holdings/sam_cph_holdings_20260710000000.parquet",
            "sam_cph_holdings/sam_cph_holdings_20260715000000.parquet");
    }

    [Fact]
    public void OrderedByTimestamp_rejects_a_key_carrying_no_timestamp()
    {
        var ordering = () => SnapshotFileNaming.OrderedByTimestamp(
            StageRunner.Definition("sam_cph_holdings"), ["sam_cph_holdings/sam_cph_holdings.parquet"]);

        ordering.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void OrderedByTimestamp_rejects_two_keys_sharing_a_timestamp()
    {
        var ordering = () => SnapshotFileNaming.OrderedByTimestamp(
            StageRunner.Definition("sam_cph_holdings"),
            [
                "sam_cph_holdings/sam_cph_holdings_20260701000000.parquet",
                "sam_cph_holdings/other_20260701000000.parquet"
            ]);

        ordering.Should().Throw<InvalidOperationException>()
            .WithMessage("*no rule for which to apply first*");
    }
}
