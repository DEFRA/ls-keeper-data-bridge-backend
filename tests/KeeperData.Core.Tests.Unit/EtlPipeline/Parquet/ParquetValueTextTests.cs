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
    public void DateTimeOffset_round_trips_in_the_O_form()
    {
        var value = new DateTimeOffset(2025, 11, 13, 12, 13, 33, TimeSpan.Zero);

        ParquetValueText.Format(value).Should().Be("2025-11-13T12:13:33.0000000+00:00");
        ParquetValueText.Parse(typeof(DateTimeOffset), "2025-11-13T12:13:33.0000000+00:00").Should().Be(value);
    }

    [Fact]
    public void TimeSpan_round_trips_in_the_constant_form()
    {
        var value = TimeSpan.FromMinutes(90);

        ParquetValueText.Format(value).Should().Be("01:30:00");
        ParquetValueText.Parse(typeof(TimeSpan), "01:30:00").Should().Be(value);
    }

    [Fact]
    public void Guid_round_trips_in_the_D_form()
    {
        var value = Guid.NewGuid();

        ParquetValueText.Format(value).Should().Be(value.ToString("D"));
        ParquetValueText.Parse(typeof(Guid), ParquetValueText.Format(value)).Should().Be(value);
    }

    [Fact]
    public void Byte_arrays_round_trip_as_base64()
    {
        var value = new byte[] { 1, 2, 3, 255 };

        ParquetValueText.Format(value).Should().Be("AQID/w==");
        ParquetValueText.Parse(typeof(byte[]), "AQID/w==").Should().BeEquivalentTo(value);
    }

    [Theory]
    [InlineData(1.5f)]
    public void Float_round_trips(float value)
    {
        ParquetValueText.Parse(typeof(float), ParquetValueText.Format(value)).Should().Be(value);
    }

    [Theory]
    [InlineData((short)5, "5")]
    [InlineData((byte)7, "7")]
    public void Smaller_integers_round_trips(object value, string text)
    {
        ParquetValueText.Format(value).Should().Be(text);
        ParquetValueText.Parse(value.GetType(), text).Should().Be(value);
    }

    [Fact]
    public void Other_iconvertible_types_use_their_invariant_form()
    {
        ParquetValueText.Format(5u).Should().Be("5");
        ParquetValueText.Format('x').Should().Be("x");
    }

    [Fact]
    public void A_non_iconvertible_value_uses_ToString()
    {
        ParquetValueText.Format(new Version(1, 2, 3)).Should().Be("1.2.3");
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

    [Fact]
    public void TryParse_reports_unconvertible_text_without_throwing()
    {
        ParquetValueText.TryParse(typeof(long), "123", out var parsed).Should().BeTrue();
        parsed.Should().Be(123L);

        ParquetValueText.TryParse(typeof(long), "abc", out _).Should().BeFalse();
        ParquetValueText.TryParse(typeof(long), "99999999999999999999", out _).Should().BeFalse("overflow is still unconvertible");
        ParquetValueText.TryParse(typeof(long), null, out var nullParsed).Should().BeTrue();
        nullParsed.Should().BeNull();
    }

    [Fact]
    public void TryParse_still_throws_for_a_type_with_no_parser()
    {
        var parse = () => ParquetValueText.TryParse(typeof(Version), "1.0", out _);

        parse.Should().Throw<InvalidOperationException>();
    }
}
