using FluentAssertions;
using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.Tests.Unit.ETL;

public class DataSetFileNamingTests
{
    private const string CtsBulkKey = "cads/cts/bulk/CTSM_CADS_PROD_BULK_00001_001_CT_LOCATION_IDENTIFIERS_2026-08-22-072824.csv";
    private const string CtsDailyKey = "cads/cts/daily/CTSM_CADS_PROD_DELTA_00002_001_CT_LOCATION_IDENTIFIERS_2026-08-23-063010.csv";

    private static readonly DataSetDefinition Cts = new(
        "cts_location_identifiers",
        "cads/cts/",
        ["LID_ID"],
        "LID_AUD_TYPE",
        [],
        DateTimePattern: "yyyy-MM-dd-HHmmss",
        SourceKeyPattern: "cads/cts/{bulk,daily}/*CT_LOCATION_IDENTIFIERS*",
        BaselineKeyPattern: "cads/cts/bulk/*CT_LOCATION_IDENTIFIERS*");

    private static readonly DataSetDefinition Litprd =
        new("sam_cph_holding", "litprd/LITP_SAMCPHHOLDING_{0}", ["CPH"], "CHANGE_TYPE", []);

    [Fact]
    public void ListingPrefixes_ForAGlobDataSet_YieldsEachLaneSeparately()
        => DataSetFileNaming.ListingPrefixes(Cts).Should().Equal("cads/cts/bulk/", "cads/cts/daily/");

    [Fact]
    public void ListingPrefixes_ForALiteralDataSet_YieldsItsSinglePrefix()
        => DataSetFileNaming.ListingPrefixes(Litprd).Should().Equal("litprd/LITP_SAMCPHHOLDING_");

    [Theory]
    [InlineData(CtsBulkKey)]
    [InlineData(CtsDailyKey)]
    [InlineData("cads/cts/bulk/PROD_BULK_BLAH_0001_CT_LOCATION_IDENTIFIERS_2026-08-22-072826.xsvn.csv")]
    public void Matches_AcceptsTheDataSetsOwnKeys(string key)
        => DataSetFileNaming.Matches(Cts, key).Should().BeTrue();

    [Theory]
    [InlineData("cads/cts/daily/CTSM_CADS_PROD_DELTA_00002_001_CT_ADDRESSES_2026-08-23-063010.csv")]
    [InlineData("cads/cts/daily/CT_ADDRESSES.csv")]
    [InlineData("cads/cts/weekly/CTSM_CADS_PROD_DELTA_00002_001_CT_LOCATION_IDENTIFIERS_2026-08-23-063010.csv")]
    [InlineData("litprd/LITP_SAMCPHHOLDING_20260822120000.csv")]
    public void Matches_RejectsAnotherTablesKeys(string key)
        => DataSetFileNaming.Matches(Cts, key).Should().BeFalse();

    /// <summary><c>*</c> stops at a segment boundary, so a lane's pattern cannot reach into a
    /// subfolder of that lane.</summary>
    [Fact]
    public void Matches_DoesNotLetASingleStarCrossASegmentBoundary()
        => DataSetFileNaming
            .Matches(Cts, "cads/cts/daily/archive/CTSM_CT_LOCATION_IDENTIFIERS_2026-08-23-063010.csv")
            .Should().BeFalse();

    [Fact]
    public void Matches_ForALiteralDataSet_UsesItsPrefix()
    {
        DataSetFileNaming.Matches(Litprd, "litprd/LITP_SAMCPHHOLDING_20260822120000.csv").Should().BeTrue();
        DataSetFileNaming.Matches(Litprd, "litprd/LITP_CTSCPHHOLDING_20260822120000.csv").Should().BeFalse();
    }

    [Fact]
    public void ExtractTimestamp_ParsesTheHyphenatedCtsFormat()
        => DataSetFileNaming.ExtractTimestamp(Cts, CtsBulkKey)
            .Should().Be(new DateTimeOffset(2026, 8, 22, 7, 28, 24, TimeSpan.Zero));

    [Fact]
    public void ExtractTimestamp_StillParsesTheLitprdFormat()
        => DataSetFileNaming.ExtractTimestamp(Litprd, "litprd/LITP_SAMCPHHOLDING_20260822120000.csv")
            .Should().Be(new DateTimeOffset(2026, 8, 22, 12, 0, 0, TimeSpan.Zero));

    /// <summary>The timestamp is read from the segment before the first dot, so a compound
    /// extension does not disturb it.</summary>
    [Fact]
    public void ExtractTimestamp_IgnoresACompoundExtension()
        => DataSetFileNaming
            .ExtractTimestamp(Cts, "cads/cts/bulk/PROD_BULK_BLAH_0001_CT_LOCATION_IDENTIFIERS_2026-08-22-072826.xsvn.csv")
            .Should().Be(new DateTimeOffset(2026, 8, 22, 7, 28, 26, TimeSpan.Zero));

    [Theory]
    [InlineData(CtsBulkKey, "00001")]
    [InlineData(CtsDailyKey, "00002")]
    [InlineData("cts_location_identifiers/CTSM_CADS_PREP_BULK_00005_011_CT_LOCATIONS_2026-08-22-072824.parquet", "00005")]
    public void ExtractRun_ReadsTheRunTheFileWasCutBy(string key, string expected)
        => DataSetFileNaming.ExtractRun(key).Should().Be(expected);

    /// <summary>A dataset naming its files any other way is one undifferentiated set, which is what the
    /// litprd feed - whose files carry no run at all - already relies on.</summary>
    [Theory]
    [InlineData("litprd/LITP_SAMCPHHOLDING_20260822120000.csv")]
    [InlineData("cads/cts/bulk/CTSM_CADS_PROD_BULK_1_1_CT_COUNTIES_2026-08-22-072824.csv")]
    public void ExtractRun_IsAbsentForAKeyNamingNoRun(string key)
        => DataSetFileNaming.ExtractRun(key).Should().BeNull();

    [Fact]
    public void ExtractTimestamp_ThrowsOnAKeyCarryingNoTimestamp()
        => FluentActions
            .Invoking(() => DataSetFileNaming.ExtractTimestamp(Cts, "cads/cts/daily/CT_ADDRESSES.csv"))
            .Should().Throw<InvalidOperationException>();
}
