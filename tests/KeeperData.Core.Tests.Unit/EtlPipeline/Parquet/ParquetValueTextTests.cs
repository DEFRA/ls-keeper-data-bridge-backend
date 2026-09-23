using FluentAssertions;
using KeeperData.Core.EtlPipeline.Parquet;

namespace KeeperData.Core.Tests.Unit.EtlPipeline.Parquet;

/// <summary>The canonical text form typed values take inside the merge. Format and Parse must
/// round-trip for every supported type, because the merge writes back what it read.</summary>
public class ParquetValueTextTests
{
    [Theory]
    [InlineData("abc")]
    [InlineData("007")]
    [InlineData("")]
    public void Strings_pass_through_unchanged(string value)
    {
        ParquetValueText.Format(value).Should().Be(value);
        ParquetValueText.Parse(typeof(string), value).Should().Be(value);
    }

    [Fact]
    public void Null_round_trips_as_null()
    {
        ParquetValueText.Format(null).Should().BeNull();
        ParquetValueText.Parse(typeof(long), null).Should().BeNull();
    }

    [Theory]
    [InlineData(123456L, "123456")]
    [InlineData(-7L, "-7")]
    public void Int64_round_trips(long value, string text)
    {
        ParquetValueText.Format(value).Should().Be(text);
        ParquetValueText.Parse(typeof(long), text).Should().Be(value);
    }

    [Fact]
    public void Int32_round_trips()
    {
        ParquetValueText.Parse(typeof(int), ParquetValueText.Format(42)).Should().Be(42);
    }

    [Theory]
    [InlineData(1.5, "1.5")]
    [InlineData(100000.0, "100000")]
    public void Double_round_trips_losslessly(double value, string text)
    {
        ParquetValueText.Format(value).Should().Be(text);
        ParquetValueText.Parse(typeof(double), text).Should().Be(value);
    }

    [Fact]
    public void Double_uses_the_round_trippable_form()
    {
        var value = 0.1 + 0.2;

        ParquetValueText.Parse(typeof(double), ParquetValueText.Format(value)).Should().Be(value);
    }

    [Fact]
    public void Decimal_round_trips()
    {
        ParquetValueText.Format(123.45m).Should().Be("123.45");
        ParquetValueText.Parse(typeof(decimal), "123.45").Should().Be(123.45m);
    }

    [Theory]
    [InlineData(true, "True")]
    [InlineData(false, "False")]
    public void Boolean_round_trips(bool value, string text)
    {
        ParquetValueText.Format(value).Should().Be(text);
        ParquetValueText.Parse(typeof(bool), text).Should().Be(value);
    }

    [Fact]
    public void DateTime_round_trips_in_the_O_form()
    {
        var value = new DateTime(2025, 11, 13, 12, 13, 33, DateTimeKind.Unspecified);

        ParquetValueText.Format(value).Should().Be("2025-11-13T12:13:33.0000000");
        ParquetValueText.Parse(typeof(DateTime), "2025-11-13T12:13:33.0000000").Should().Be(value);
    }

    [Fact]
    public void DateOnly_round_trips_in_the_O_form()
    {
        var value = new DateOnly(2025, 11, 13);

        ParquetValueText.Format(value).Should().Be("2025-11-13");
        ParquetValueText.Parse(typeof(DateOnly), "2025-11-13").Should().Be(value);
    }

    [Fact]
    public void A_nullable_target_type_is_unwrapped()
    {
        ParquetValueText.Parse(typeof(long?), "5").Should().Be(5L);
    }

    [Fact]
    public void An_unsupported_type_fails_loudly()
    {
        var parse = () => ParquetValueText.Parse(typeof(Version), "1.0");

        parse.Should().Throw<InvalidOperationException>().WithMessage("*Version*");
    }
}
