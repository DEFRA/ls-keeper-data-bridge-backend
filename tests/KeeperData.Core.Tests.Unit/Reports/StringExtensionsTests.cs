using FluentAssertions;
using KeeperData.Core.Reports;

namespace KeeperData.Core.Tests.Unit.Reports;

public class StringExtensionsTests
{
    #region ToInteger Tests

    [Theory]
    [InlineData("123", 123)]
    [InlineData("0", 0)]
    [InlineData("-456", -456)]
    [InlineData("999999999", 999999999)]
    public void ToInteger_WithValidInteger_ReturnsValue(string input, int expected)
    {
        var result = input.ToInteger();

        result.Should().Be(expected);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void ToInteger_WithNullOrWhitespace_ReturnsNull(string? input)
    {
        var result = input.ToInteger();

        result.Should().BeNull();
    }

    [Theory]
    [InlineData("abc")]
    [InlineData("12.34")]
    [InlineData("12abc")]
    [InlineData("1,234")]
    public void ToInteger_WithInvalidInteger_ReturnsNull(string input)
    {
        var result = input.ToInteger();

        result.Should().BeNull();
    }

    #endregion

    #region ToDateTime Tests

    [Fact]
    public void ToDateTime_WithValidDateAndFormat_ReturnsDateTime()
    {
        var result = "2024-06-15".ToDateTime("yyyy-MM-dd");

        result.Should().NotBeNull();
        result!.Value.Year.Should().Be(2024);
        result.Value.Month.Should().Be(6);
        result.Value.Day.Should().Be(15);
    }

    [Fact]
    public void ToDateTime_WithMultipleFormats_TriesEachFormat()
    {
        var result = "15/06/2024".ToDateTime("yyyy-MM-dd", "dd/MM/yyyy");

        result.Should().NotBeNull();
        result!.Value.Year.Should().Be(2024);
        result.Value.Month.Should().Be(6);
        result.Value.Day.Should().Be(15);
    }

    [Fact]
    public void ToDateTime_WithFirstFormatMatching_ReturnsDateTime()
    {
        var result = "2024-06-15".ToDateTime("yyyy-MM-dd", "dd/MM/yyyy");

        result.Should().NotBeNull();
        result!.Value.Year.Should().Be(2024);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void ToDateTime_WithNullOrWhitespace_ReturnsNull(string? input)
    {
        var result = input.ToDateTime("yyyy-MM-dd");

        result.Should().BeNull();
    }

    [Fact]
    public void ToDateTime_WithNoMatchingFormat_ReturnsNull()
    {
        var result = "invalid-date".ToDateTime("yyyy-MM-dd", "dd/MM/yyyy");

        result.Should().BeNull();
    }

    [Fact]
    public void ToDateTime_WithWrongFormat_ReturnsNull()
    {
        var result = "2024-13-45".ToDateTime("yyyy-MM-dd");  // Invalid month and day

        result.Should().BeNull();
    }

    [Fact]
    public void ToDateTime_WithTimeIncluded_ParsesCorrectly()
    {
        var result = "2024-06-15 14:30:00".ToDateTime("yyyy-MM-dd HH:mm:ss");

        result.Should().NotBeNull();
        result!.Value.Hour.Should().Be(14);
        result.Value.Minute.Should().Be(30);
        result.Value.Second.Should().Be(0);
    }

    [Fact]
    public void ToDateTime_WithEmptyFormatsArray_ReturnsNull()
    {
        var result = "2024-06-15".ToDateTime();

        result.Should().BeNull();
    }

    #endregion
}
