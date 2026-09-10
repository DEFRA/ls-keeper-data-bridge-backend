using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using Xunit;

namespace KeeperData.Core.Tests.Unit.ETL;

[Trait("Category", "Unit")]
public class DataSetUploadKeyTests
{
    private static readonly System.Collections.Immutable.ImmutableArray<DataSetDefinition> Definitions =
        StandardDataSetDefinitionsBuilder.Build().All;

    [Fact]
    public void FullKey_CarryingTheSourceFolder_IsAccepted()
    {
        var resolution = DataSetUploadKey.Resolve(Definitions, "litprd/LITP_SAMCPHHOLDING_20250101120000.csv");

        resolution.Key.Should().Be("litprd/LITP_SAMCPHHOLDING_20250101120000.csv");
        resolution.ErrorMessage.Should().BeNull();
    }

    [Fact]
    public void BareFileName_IsResolvedToItsDatasetsFolder()
    {
        var resolution = DataSetUploadKey.Resolve(Definitions, "LITP_SAMCPHHOLDING_20250101120000.csv");

        resolution.Key.Should().Be("litprd/LITP_SAMCPHHOLDING_20250101120000.csv");
    }

    [Theory]
    [InlineData("cads/cts/bulk/CTSM_CADS_PROD_BULK_00001_001_CT_LOCATION_IDENTIFIERS_2026-08-22-072826.csv")]
    [InlineData("cads/cts/daily/CTSM_CADS_PROD_DELTA_00002_001_CT_LOCATION_IDENTIFIERS_2026-08-23-063010.csv")]
    public void EitherCtsLane_IsAccepted_WhenTheKeyNamesIt(string key)
    {
        var resolution = DataSetUploadKey.Resolve(Definitions, key);

        resolution.Key.Should().Be(key);
    }

    [Theory]
    [InlineData(
        "CTSM_CADS_PROD_BULK_00001_001_CT_LOCATION_IDENTIFIERS_2026-08-22-072826.csv",
        "cads/cts/bulk/CTSM_CADS_PROD_BULK_00001_001_CT_LOCATION_IDENTIFIERS_2026-08-22-072826.csv")]
    [InlineData(
        "CTSM_CADS_PROD_DELTA_00002_001_CT_LOCATION_IDENTIFIERS_2026-08-23-063010.csv",
        "cads/cts/daily/CTSM_CADS_PROD_DELTA_00002_001_CT_LOCATION_IDENTIFIERS_2026-08-23-063010.csv")]
    public void BareCtsFileName_TakesTheLaneItsNameClaims(string fileName, string expected)
    {
        var resolution = DataSetUploadKey.Resolve(Definitions, fileName);

        resolution.Key.Should().Be(expected);
    }

    [Fact]
    public void BareCtsFileName_CarryingNoBulkMarker_IsTreatedAsADelta()
    {
        var resolution = DataSetUploadKey.Resolve(
            Definitions, "CTSM_CADS_PROD_00001_001_CT_LOCATION_IDENTIFIERS_2026-08-23-063010.csv");

        resolution.Key.Should().StartWith("cads/cts/daily/");
    }

    [Fact]
    public void FullKey_PlacingABulkFileInTheDailyLane_IsStillAccepted()
    {
        const string key = "cads/cts/daily/CTSM_CADS_PROD_BULK_00001_001_CT_LOCATION_IDENTIFIERS_2026-08-22-072826.csv";

        DataSetUploadKey.Resolve(Definitions, key).Key.Should().Be(key);
    }

    [Fact]
    public void KeyMatchingNoDataset_IsRefused_WithTheRegisteredPatterns()
    {
        var resolution = DataSetUploadKey.Resolve(Definitions, "litprd/LITP_NOTADATASET_20250101120000.csv");

        resolution.Key.Should().BeNull();
        resolution.ErrorMessage.Should()
            .Contain("litprd/LITP_SAMCPHHOLDING_yyyyMMddHHmmss.csv")
            .And.Contain("cads/cts/{bulk,daily}/");
    }

    [Fact]
    public void KeyMatchingADataset_WithAnUnparsableTimestamp_IsRefused()
    {
        var resolution = DataSetUploadKey.Resolve(Definitions, "litprd/LITP_SAMCPHHOLDING_notatimestamp.csv");

        resolution.Key.Should().BeNull();
        resolution.ErrorMessage.Should().Contain("sam_cph_holdings");
    }

    [Fact]
    public void CtsKeyCarryingTheLitprdTimestampFormat_IsRefused()
    {
        var resolution = DataSetUploadKey.Resolve(
            Definitions, "cads/cts/bulk/CTSM_CADS_PROD_BULK_00001_001_CT_LOCATION_IDENTIFIERS_20250101120000.csv");

        resolution.Key.Should().BeNull();
        resolution.ErrorMessage.Should().Contain("cts_location_identifiers");
    }

    [Theory]
    [InlineData("litprd\\LITP_SAMCPHHOLDING_20250101120000.csv")]
    [InlineData("litprd//LITP_SAMCPHHOLDING_20250101120000.csv")]
    [InlineData("/litprd/LITP_SAMCPHHOLDING_20250101120000.csv")]
    [InlineData("litprd/../LITP_SAMCPHHOLDING_20250101120000.csv")]
    public void MalformedKeys_AreRefused(string key)
    {
        var resolution = DataSetUploadKey.Resolve(Definitions, key);

        resolution.Key.Should().BeNull();
        resolution.ErrorMessage.Should().NotBeNullOrEmpty();
    }
}
