using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Optimise;

namespace KeeperData.Core.Tests.Unit.EtlPipeline.Optimise;

/// <summary>The strict, invariant conversion of one source string to its resolved type. A value
/// that does not parse throws FormatException, which the optimiser wraps in
/// SourceFileConversionException with the file, column and record position.</summary>
public class ColumnValueParserTests
{
    private const byte Precision = 10;
    private const byte Scale = 2;

    [Theory]
    [InlineData(ColumnDataType.Int64)]
    [InlineData(ColumnDataType.Double)]
    [InlineData(ColumnDataType.Boolean)]
    [InlineData(ColumnDataType.Date)]
    [InlineData(ColumnDataType.Timestamp)]
    [InlineData(ColumnDataType.Decimal)]
    public void Null_and_empty_parse_to_null_for_every_typed_target(ColumnDataType type)
    {
        ColumnValueParser.Parse(null, type, Precision, Scale).Should().BeNull();
        ColumnValueParser.Parse("", type, Precision, Scale).Should().BeNull();
    }

    [Fact]
    public void An_unknown_target_type_fails_loudly()
    {
        var parse = () => ColumnValueParser.Parse("1", (ColumnDataType)99, Precision, Scale);

        parse.Should().Throw<InvalidOperationException>().WithMessage("*99*");
    }

    [Fact]
    public void String_passes_the_value_through_verbatim()
    {
        ColumnValueParser.Parse("007", ColumnDataType.String, Precision, Scale).Should().Be("007");
        ColumnValueParser.Parse("", ColumnDataType.String, Precision, Scale).Should().Be("");
    }

    [Theory]
    [InlineData("123", 123L)]
    [InlineData("-45", -45L)]
    [InlineData("007", 7L)]
    public void Parses_integers(string value, long expected)
    {
        ColumnValueParser.Parse(value, ColumnDataType.Int64, Precision, Scale).Should().Be(expected);
    }

    [Theory]
    [InlineData("abc")]
    [InlineData("1.5")]
    public void Rejects_a_non_integer_for_Int64(string value)
    {
        var parse = () => ColumnValueParser.Parse(value, ColumnDataType.Int64, Precision, Scale);

        parse.Should().Throw<FormatException>();
    }

    [Fact]
    public void Rejects_an_integer_that_overflows_Int64()
    {
        var parse = () => ColumnValueParser.Parse("99999999999999999999", ColumnDataType.Int64, Precision, Scale);

        parse.Should().Throw<FormatException>();
    }

    [Theory]
    [InlineData("1.5", 1.5)]
    [InlineData("1e5", 100000.0)]
    [InlineData("-0.25", -0.25)]
    public void Parses_reals(string value, double expected)
    {
        ColumnValueParser.Parse(value, ColumnDataType.Double, Precision, Scale).Should().Be(expected);
    }

    [Theory]
    [InlineData("true", true)]
    [InlineData("FALSE", false)]
    [InlineData("1", true)]
    [InlineData("0", false)]
    public void Parses_booleans(string value, bool expected)
    {
        ColumnValueParser.Parse(value, ColumnDataType.Boolean, Precision, Scale).Should().Be(expected);
    }

    [Fact]
    public void Rejects_a_boolean_token_it_does_not_know()
    {
        var parse = () => ColumnValueParser.Parse("yes", ColumnDataType.Boolean, Precision, Scale);

        parse.Should().Throw<FormatException>();
    }

    [Theory]
    [InlineData("2025-11-13")]
    [InlineData("20251113")]
    public void Parses_dates_in_both_source_forms(string value)
    {
        ColumnValueParser.Parse(value, ColumnDataType.Date, Precision, Scale)
            .Should().Be(new DateOnly(2025, 11, 13));
    }

    [Fact]
    public void Rejects_a_date_in_a_local_format()
    {
        var parse = () => ColumnValueParser.Parse("13/11/2025", ColumnDataType.Date, Precision, Scale);

        parse.Should().Throw<FormatException>();
    }

    [Theory]
    [InlineData("2025-11-13T12:13:33")]
    [InlineData("2025-11-13 12:13:33")]
    [InlineData("20251113121333")]
    [InlineData("2025-11-13T12:13:33.1234567")]
    public void Parses_timestamps(string value)
    {
        ColumnValueParser.Parse(value, ColumnDataType.Timestamp, Precision, Scale)
            .Should().Be(new DateTime(2025, 11, 13, 12, 13, 33).AddTicks(value.Contains('.') ? 1234567 : 0));
    }

    [Fact]
    public void A_bare_date_parses_as_a_midnight_timestamp()
    {
        ColumnValueParser.Parse("2025-11-13", ColumnDataType.Timestamp, Precision, Scale)
            .Should().Be(new DateTime(2025, 11, 13));
    }

    [Fact]
    public void Parses_a_decimal_within_the_declared_precision_and_scale()
    {
        ColumnValueParser.Parse("123.45", ColumnDataType.Decimal, Precision, Scale).Should().Be(123.45m);
    }

    [Fact]
    public void Rejects_a_decimal_with_more_fraction_digits_than_the_scale()
    {
        var parse = () => ColumnValueParser.Parse("123.456", ColumnDataType.Decimal, Precision, Scale);

        parse.Should().Throw<FormatException>(
            "parquet carries the digits and a reader applies (p, s) on the way out, so an overflow is silently wrong downstream");
    }

    [Fact]
    public void Rejects_a_decimal_with_more_integer_digits_than_precision_minus_scale()
    {
        // Precision 10, scale 2: at most 8 integer digits.
        var parse = () => ColumnValueParser.Parse("123456789.00", ColumnDataType.Decimal, Precision, Scale);

        parse.Should().Throw<FormatException>();
    }

    [Fact]
    public void Rejects_a_value_that_is_not_numeric_for_Decimal()
    {
        var parse = () => ColumnValueParser.Parse("abc", ColumnDataType.Decimal, Precision, Scale);

        parse.Should().Throw<FormatException>();
    }
}
