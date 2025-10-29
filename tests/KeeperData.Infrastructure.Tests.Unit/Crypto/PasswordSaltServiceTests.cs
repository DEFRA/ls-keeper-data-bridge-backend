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
}