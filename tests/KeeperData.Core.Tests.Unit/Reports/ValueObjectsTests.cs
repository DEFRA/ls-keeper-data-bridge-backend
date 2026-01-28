using FluentAssertions;
using KeeperData.Core.Reports;

namespace KeeperData.Core.Tests.Unit.Reports;

public class CphTests
{
    [Theory]
    [InlineData("12/345/6789", "12", "345", "6789")]
    [InlineData("01/001/0001", "01", "001", "0001")]
    [InlineData("99/999/9999", "99", "999", "9999")]
    public void Parse_WithValidFormat_ReturnsCph(string value, string expectedCounty, string expectedParish, string expectedHolding)
    {
        var cph = Cph.Parse(value);

        cph.Value.Should().Be(value);
        cph.CountyCode.Should().Be(expectedCounty);
        cph.ParishCode.Should().Be(expectedParish);
        cph.HoldingCode.Should().Be(expectedHolding);
    }

    [Fact]
    public void Parse_WithNullValue_ThrowsArgumentNullException()
    {
        var act = () => Cph.Parse(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("12345")]
    [InlineData("12/345")]
    [InlineData("12/345/6789/extra")]
    [InlineData("/345/6789")]
    [InlineData("12//6789")]
    [InlineData("12/345/")]
    public void Parse_WithInvalidFormat_ThrowsFormatException(string value)
    {
        var act = () => Cph.Parse(value);

        act.Should().Throw<FormatException>()
            .WithMessage("*not a valid CPH*");
    }

    [Theory]
    [InlineData("12/345/6789", "12", "345", "6789")]
    [InlineData("01/001/0001", "01", "001", "0001")]
    public void TryParse_WithValidFormat_ReturnsCph(string value, string expectedCounty, string expectedParish, string expectedHolding)
    {
        var cph = Cph.TryParse(value);

        cph.Should().NotBeNull();
        cph!.Value.Should().Be(value);
        cph.CountyCode.Should().Be(expectedCounty);
        cph.ParishCode.Should().Be(expectedParish);
        cph.HoldingCode.Should().Be(expectedHolding);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("invalid")]
    [InlineData("12/345")]
    [InlineData("12/345/6789/extra")]
    public void TryParse_WithInvalidFormat_ReturnsNull(string? value)
    {
        var cph = Cph.TryParse(value);

        cph.Should().BeNull();
    }

    [Fact]
    public void ToString_ReturnsCanonicalFormat()
    {
        var cph = Cph.Parse("12/345/6789");

        cph.ToString().Should().Be("12/345/6789");
    }
}

public class LidFullIdentifierTests
{
    [Theory]
    [InlineData("EN-12/345/6789", "EN", "12", "345", "6789")]
    [InlineData("WA-01/001/0001", "WA", "01", "001", "0001")]
    [InlineData("SC-99/999/9999", "SC", "99", "999", "9999")]
    public void Parse_WithValidFormat_ReturnsLidFullIdentifier(
        string value, string expectedRegion, string expectedCounty, string expectedParish, string expectedHolding)
    {
        var lid = LidFullIdentifier.Parse(value);

        lid.Value.Should().Be(value);
        lid.Region.Should().Be(expectedRegion);
        lid.Cph.CountyCode.Should().Be(expectedCounty);
        lid.Cph.ParishCode.Should().Be(expectedParish);
        lid.Cph.HoldingCode.Should().Be(expectedHolding);
    }

    [Fact]
    public void Parse_WithNullValue_ThrowsArgumentNullException()
    {
        var act = () => LidFullIdentifier.Parse(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("12/345/6789")]  // Missing region
    [InlineData("-12/345/6789")] // Empty region
    [InlineData("EN-")]          // Missing CPH
    [InlineData("EN-invalid")]   // Invalid CPH
    [InlineData("EN12/345/6789")] // Missing hyphen
    public void Parse_WithInvalidFormat_ThrowsFormatException(string value)
    {
        var act = () => LidFullIdentifier.Parse(value);

        act.Should().Throw<FormatException>()
            .WithMessage("*not a valid LID full identifier*");
    }

    [Theory]
    [InlineData("EN-12/345/6789", "EN", "12/345/6789")]
    [InlineData("WA-01/001/0001", "WA", "01/001/0001")]
    public void TryParse_WithValidFormat_ReturnsLidFullIdentifier(string value, string expectedRegion, string expectedCph)
    {
        var lid = LidFullIdentifier.TryParse(value);

        lid.Should().NotBeNull();
        lid!.Value.Should().Be(value);
        lid.Region.Should().Be(expectedRegion);
        lid.Cph.Value.Should().Be(expectedCph);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("invalid")]
    [InlineData("EN-invalid")]
    [InlineData("-12/345/6789")]
    public void TryParse_WithInvalidFormat_ReturnsNull(string? value)
    {
        var lid = LidFullIdentifier.TryParse(value);

        lid.Should().BeNull();
    }

    [Fact]
    public void ToString_ReturnsCanonicalFormat()
    {
        var lid = LidFullIdentifier.Parse("EN-12/345/6789");

        lid.ToString().Should().Be("EN-12/345/6789");
    }

    [Fact]
    public void Cph_IsAccessible()
    {
        var lid = LidFullIdentifier.Parse("EN-12/345/6789");

        lid.Cph.Should().NotBeNull();
        lid.Cph.Value.Should().Be("12/345/6789");
    }
}
