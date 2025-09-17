using KeeperData.Core.Exceptions;
using KeeperData.Infrastructure.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Time.Testing;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.Services;

public class PasswordSaltServiceTests
{
    private readonly Mock<IConfiguration> _configurationMock;
    private readonly FakeTimeProvider _fakeTimeProvider;
    private readonly PasswordSaltService _sut;
    private readonly string _testSalt = "TestSaltValue123";

    public PasswordSaltServiceTests()
    {
        _configurationMock = new Mock<IConfiguration>();
        _configurationMock.Setup(x => x["AesSalt"]).Returns(_testSalt);

        _fakeTimeProvider = new FakeTimeProvider();
        _fakeTimeProvider.SetUtcNow(new DateTimeOffset(2025, 8, 5, 14, 30, 45, TimeSpan.Zero));

        _sut = new PasswordSaltService(_configurationMock.Object, _fakeTimeProvider);
    }

    [Fact]
    public void Constructor_WithNullConfiguration_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new PasswordSaltService(null!, _fakeTimeProvider));
    }

    [Fact]
    public void Constructor_WithNullTimeProvider_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new PasswordSaltService(_configurationMock.Object, null!));
    }

    [Theory]
    [InlineData("CTSM_UKV_PROD_DELTA_01628_CT_ADDRESSES_2025-08-05-060014.csv", "2025-08-05_ADDRESSES_CT_01628_DELTA_PROD_UKV_CTSM_060014.csv")]
    [InlineData("ABC_DEF_2025-08-05.txt", "2025-08-05_DEF_ABC.txt")]
    [InlineData("ONE_TWO_THREE_2025-08-05-123456.json", "2025-08-05_THREE_TWO_ONE_123456.json")]
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
        var service = new PasswordSaltService(emptyConfigMock.Object, _fakeTimeProvider);

        var ex = Assert.Throws<InvalidOperationException>(() => service.Get("test_2025-08-05.csv"));
        Assert.Equal("AesSalt configuration value is missing or empty.", ex.Message);
    }

    [Theory]
    [InlineData("filenamewithoutunderscores2025-08-05.csv")]
    [InlineData("filename_without_date.csv")]
    [InlineData("filename_with_partial_date_2025-08.csv")]
    [InlineData("filename_with_no_date_pattern.csv")]
    public void Get_WithInvalidFileName_ThrowsInvalidFileNameFormatException(string fileName)
    {
        Assert.Throws<InvalidFileNameFormatException>(() => _sut.Get(fileName));
    }

    [Fact]
    public void GenerateFileName_GeneratesValidFileName()
    {
        var result = _sut.GenerateFileName();

        Assert.NotNull(result);
        Assert.EndsWith("_2025-08-05-143045.csv", result);

        var parts = result.Split('_');
        Assert.True(parts.Length >= 8);        

        var passwordSalt = _sut.Get(result);
        Assert.NotNull(passwordSalt);
        Assert.Equal(_testSalt, passwordSalt.Salt);
    }

    [Fact]
    public void GenerateFileName_GeneratesRandomPrefixes()
    {
        var result1 = _sut.GenerateFileName();
        var result2 = _sut.GenerateFileName();

        Assert.NotEqual(result1, result2);

        Assert.EndsWith("_2025-08-05-143045.csv", result1);
        Assert.EndsWith("_2025-08-05-143045.csv", result2);

        var prefix1 = result1.Replace("_2025-08-05-143045.csv", "");
        var prefix2 = result2.Replace("_2025-08-05-143045.csv", "");
        Assert.NotEqual(prefix1, prefix2);
    }

    [Fact]
    public void GenerateFileName_PrefixesContainOnlyUppercaseAndNumbers()
    {
        var result = _sut.GenerateFileName();

        var withoutDateAndExtension = result.Replace("_2025-08-05-143045.csv", "");
        var prefixes = withoutDateAndExtension.Split('_');

        foreach (var prefix in prefixes)
        {
            Assert.Matches("^[A-Z0-9]+$", prefix);
            Assert.InRange(prefix.Length, 2, 9);
        }
    }

    [Fact]
    public void Get_WithFileNameWithoutExtension_HandlesCorrectly()
    {
        var fileName = "TEST_FILE_2025-08-05";

        var result = _sut.Get(fileName);

        Assert.Equal("2025-08-05_FILE_TEST", result.Password);
        Assert.Equal(_testSalt, result.Salt);
    }

    [Fact]
    public void Get_WithMultipleDatesInFileName_UsesFirstMatch()
    {
        var fileName = "TEST_2025-08-05_DATA_2025-09-06.csv";

        var result = _sut.Get(fileName);

        Assert.Equal("2025-08-05_TEST_2025-09-06_DATA.csv", result.Password);
    }
}