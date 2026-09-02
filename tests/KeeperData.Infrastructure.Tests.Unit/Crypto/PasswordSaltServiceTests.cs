using KeeperData.Core.ETL.Impl;
using KeeperData.Infrastructure.Crypto;
using Microsoft.Extensions.Configuration;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Crypto;

public class PasswordSaltServiceTests
{
    private readonly Mock<IConfiguration> _configurationMock;
    private readonly PasswordSaltService _sut;
    private readonly string _testSalt = "TestSaltValue123";

    public PasswordSaltServiceTests()
    {
        _configurationMock = new Mock<IConfiguration>();
        _configurationMock.Setup(x => x["AesSalt"]).Returns(_testSalt);


        _sut = new PasswordSaltService(_configurationMock.Object);
    }

    [Fact]
    public void Constructor_WithNullConfiguration_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new PasswordSaltService(null!));
    }

    [Theory]
    [InlineData("CTSM_UKV_PROD_DELTA_01628_CT_ADDRESSES_2025-08-05-060014.csv", "CTSM_UKV_PROD_DELTA_01628_CT_ADDRESSES_2025-08-05-060014.csv")]
    public void Get_WithValidFileName_ReturnsCorrectPasswordAndSalt(string fileName, string expectedPassword)
    {
        var result = _sut.Get(fileName);

        Assert.Equal(expectedPassword, result.Password);
        Assert.Equal(_testSalt, result.Salt);
    }

    [Fact]
    public void Get_WithNullOrWhiteSpaceFileName_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => _sut.Get(null!));
        Assert.Throws<ArgumentNullException>(() => _sut.Get(""));
        Assert.Throws<ArgumentNullException>(() => _sut.Get("   "));
    }

    [Fact]
    public void Get_WhenAesSaltNotConfigured_ThrowsInvalidOperationException()
    {
        var emptyConfigMock = new Mock<IConfiguration>();
        emptyConfigMock.Setup(x => x["AesSalt"]).Returns((string)null!);
        var service = new PasswordSaltService(emptyConfigMock.Object);

        var ex = Assert.Throws<InvalidOperationException>(() => service.Get("test_2025-08-05.csv"));
        Assert.Equal("AesSalt configuration value is missing or empty.", ex.Message);
    }


    [Fact]
    public void Get_WithFileNameWithoutExtension_HandlesCorrectly()
    {
        var fileName = "TEST_FILE_2025-08-05";

        var result = _sut.Get(fileName);

        Assert.Equal(fileName, result.Password);
        Assert.Equal(_testSalt, result.Salt);
    }

    [Theory]
    [InlineData(
        "CTSM_CADS_PROD_BULK_00001_001_CT_LOCATION_IDENTIFIERS_2026-08-22-072824.csv",
        "2026-08-22_IDENTIFIERS_LOCATION_CT_001_00001_BULK_PROD_CADS_CTSM")]
    [InlineData(
        "CTSM_CADS_PROD_BULK_00001_001_CT_LOCATION_IDENTIFIERS_2026-08-22-072824.csv.enc",
        "2026-08-22_IDENTIFIERS_LOCATION_CT_001_00001_BULK_PROD_CADS_CTSM")]
    [InlineData(
        "CTSM_UKV_PROD_DELTA_01628_CT_LOCATION_IDENTIFIERS_2026-08-22-072824.csv.enc",
        "2026-08-22_IDENTIFIERS_LOCATION_CT_01628_DELTA_PROD_UKV_CTSM")]
    [InlineData(
        "CTSM_CADS_PROD_BULK_00001_001_CT_LOCATION_IDENTIFIERS_2026-08-22-072824",
        "2026-08-22_IDENTIFIERS_LOCATION_CT_001_00001_BULK_PROD_CADS_CTSM")]
    [InlineData(
        "CTSM_CADS_PROD_DELTA_00002_001_CT_LOCATION_IDENTIFIERS_2026-08-23-063010.xsvn.csv",
        "2026-08-23_IDENTIFIERS_LOCATION_CT_001_00002_DELTA_PROD_CADS_CTSM")]
    [InlineData(
        "CTSM_CADS_PROD_DELTA_00002_001_CT_LOCATION_IDENTIFIERS_2026-08-23-063010.xsvn.csv.enc",
        "2026-08-23_IDENTIFIERS_LOCATION_CT_001_00002_DELTA_PROD_CADS_CTSM")]
    [InlineData(
        "CT_LOCATION_IDENTIFIERS_2026-08-22-072826.xsvn.csv",
        "2026-08-22_IDENTIFIERS_LOCATION_CT")]
    public void Get_WithCtsDerivedPolicy_DerivesThePasswordFromTheName(string fileName, string expectedPassword)
    {
        var result = _sut.Get(fileName, PasswordDerivationPolicy.CtsDerived);

        Assert.Equal(expectedPassword, result.Password);
        Assert.Equal(_testSalt, result.Salt);
    }

    [Fact]
    public void Get_WithFileNameVerbatimPolicy_ReturnsACtsShapedNameUnchanged()
    {
        var fileName = "CTSM_CADS_PROD_BULK_00001_001_CT_LOCATION_IDENTIFIERS_2026-08-22-072824.csv.enc";

        var result = _sut.Get(fileName, PasswordDerivationPolicy.FileNameVerbatim);

        Assert.Equal(fileName, result.Password);
    }

    [Theory]
    [InlineData(PasswordDerivationPolicy.FileNameVerbatim, "LITP_SAMHERD_2025-08-05-060014.csv.enc")]
    [InlineData(PasswordDerivationPolicy.CtsDerived, "CTSM_CADS_PROD_BULK_2026-08-22-072824.csv.enc")]
    [InlineData(PasswordDerivationPolicy.CtsDerived, "CT_LOCATION_IDENTIFIERS_2026-08-22-072826.xsvn.csv")]
    public void Get_WithAFolderedKey_DerivesTheSamePasswordAsTheBareName(PasswordDerivationPolicy policy, string fileName)
    {
        var expected = _sut.Get(fileName, policy).Password;

        Assert.Equal(expected, _sut.Get($"litprd/{fileName}", policy).Password);
        Assert.Equal(expected, _sut.Get($"litprd/nested/folder/{fileName}", policy).Password);
    }

    [Theory]
    [InlineData("2026-08-22-072824.csv")]
    [InlineData("CTSM.csv")]
    public void Get_WithCtsDerivedPolicyAndTooFewSegments_Throws(string fileName)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => _sut.Get(fileName, PasswordDerivationPolicy.CtsDerived));

        Assert.Contains("no underscore-separated segments", ex.Message);
    }

    [Theory]
    [InlineData("CTSM_CADS_2026-08-22.csv")]
    [InlineData("CTSM_CADS_2026-08-22-072824extra.csv")]
    [InlineData("CTSM_CADS_20260822-072824.csv")]
    [InlineData("CTSM_CADS_IDENTIFIERS.csv")]
    public void Get_WithCtsDerivedPolicyAndNoTrailingTimestamp_Throws(string fileName)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => _sut.Get(fileName, PasswordDerivationPolicy.CtsDerived));

        Assert.Contains("is not a yyyy-MM-dd-HHmmss timestamp", ex.Message);
    }

    [Fact]
    public void Get_WithAnUnknownPolicy_Throws()
    {
        Assert.Throws<NotSupportedException>(
            () => _sut.Get("CTSM_CADS_2026-08-22-072824.csv", (PasswordDerivationPolicy)99));
    }

    [Fact]
    public void Get_WithCtsDerivedPolicyAndNoAesSalt_ThrowsBeforeDeriving()
    {
        var emptyConfigMock = new Mock<IConfiguration>();
        emptyConfigMock.Setup(x => x["AesSalt"]).Returns((string)null!);
        var service = new PasswordSaltService(emptyConfigMock.Object);

        var ex = Assert.Throws<InvalidOperationException>(
            () => service.Get("not-a-cts-name", PasswordDerivationPolicy.CtsDerived));

        Assert.Equal("AesSalt configuration value is missing or empty.", ex.Message);
    }
}