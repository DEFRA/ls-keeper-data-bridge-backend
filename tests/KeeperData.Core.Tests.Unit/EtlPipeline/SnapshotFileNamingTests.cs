using FluentAssertions;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

public class SnapshotFileNamingTests
{
    [Fact]
    public void SnapshotKey_uses_the_clean_dataset_name_and_the_etl_timestamp()
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
}
