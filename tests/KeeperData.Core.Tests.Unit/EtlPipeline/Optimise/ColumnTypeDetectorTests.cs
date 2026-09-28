using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Optimise;

namespace KeeperData.Core.Tests.Unit.EtlPipeline.Optimise;

/// <summary>The type a sampled column resolves to. Every rule exists to stop a value being
/// retyped into something that loses information: a leading-zero code is not a number, a 20-digit
/// reference is not a double, and no sample ever produces Decimal.</summary>
public class ColumnTypeDetectorTests
{
    [Theory]
    [InlineData("123")]
    [InlineData("-45")]
    [InlineData("0")]
    public void Detects_integers_as_Int64(string value)
    {
        ColumnTypeDetector.Detect([value]).Should().Be(ColumnDataType.Int64);
    }

    [Theory]
    [InlineData("007")]
    [InlineData("01")]
    [InlineData("+7")]
    [InlineData(" 1")]
    [InlineData("1 ")]
    public void Leaves_an_integer_shaped_code_a_string(string value)
    {
        ColumnTypeDetector.Detect([value]).Should().Be(ColumnDataType.String,
            "'{0}' carries information Int64 cannot hold, so it must not be converted", value);
    }

    [Fact]
    public void An_int_like_value_too_big_for_Int64_stays_a_string_not_a_double()
    {
        ColumnTypeDetector.Detect(["99999999999999999999"]).Should().Be(ColumnDataType.String,
            "a 20-digit reference number must not degrade to 1E+19");
    }

    [Theory]
    [InlineData("1.5")]
    [InlineData("-0.25")]
    [InlineData("1e5")]
    [InlineData("2.5E-3")]
    public void Detects_reals_as_Double(string value)
    {
        ColumnTypeDetector.Detect([value]).Should().Be(ColumnDataType.Double);
    }

    [Fact]
    public void A_column_mixing_integers_and_reals_resolves_to_Double()
    {
        ColumnTypeDetector.Detect(["7", "1.5"]).Should().Be(ColumnDataType.Double);
    }

    [Theory]
    [InlineData("true")]
    [InlineData("FALSE")]
    [InlineData("True")]
    public void Detects_boolean_tokens_as_Boolean(string value)
    {
        ColumnTypeDetector.Detect([value]).Should().Be(ColumnDataType.Boolean);
    }

    [Theory]
    [InlineData("1")]
    [InlineData("0")]
    public void Digits_are_ints_before_they_are_booleans(string value)
    {
        ColumnTypeDetector.Detect([value]).Should().Be(ColumnDataType.Int64,
            "'{0}' is claimed by Int64 first, so a code column of 0/1 stays numeric rather than flipping to flags", value);
    }

    [Fact]
    public void Detects_a_bare_date_as_Date()
    {
        ColumnTypeDetector.Detect(["2025-11-13"]).Should().Be(ColumnDataType.Date);
    }

    [Fact]
    public void An_eight_digit_date_is_an_integer_not_a_date()
    {
        ColumnTypeDetector.Detect(["20251113"]).Should().Be(ColumnDataType.Int64,
            "pure digits are claimed by Int64 before the date check runs");
    }

    [Theory]
    [InlineData("2025-11-13T12:13:33")]
    [InlineData("2025-11-13 12:13:33")]
    [InlineData("2025-11-13T12:13:33.1234567")]
    public void Detects_a_date_time_as_Timestamp(string value)
    {
        ColumnTypeDetector.Detect([value]).Should().Be(ColumnDataType.Timestamp);
    }

    [Fact]
    public void A_column_mixing_dates_and_timestamps_stays_a_string()
    {
        ColumnTypeDetector.Detect(["2025-11-13", "2025-11-14T00:00:00"]).Should().Be(ColumnDataType.String,
            "neither candidate survives both values, and guessing either loses the other");
    }

    [Fact]
    public void A_column_that_is_entirely_null_stays_a_string()
    {
        ColumnTypeDetector.Detect([null, "", null]).Should().Be(ColumnDataType.String);
    }

    [Fact]
    public void Nulls_do_not_vote_against_the_type_the_values_have()
    {
        ColumnTypeDetector.Detect([null, "42", ""]).Should().Be(ColumnDataType.Int64);
    }

    [Fact]
    public void One_unconvertible_value_keeps_the_column_a_string()
    {
        ColumnTypeDetector.Detect(["1", "2", "three"]).Should().Be(ColumnDataType.String);
    }

    [Fact]
    public void Detection_never_produces_Decimal()
    {
        ColumnTypeDetector.Detect(["123.45", "678.90"]).Should().Be(ColumnDataType.Double,
            "Decimal is only reachable through an explicit ColumnTypes entry - precision cannot be inferred");
    }
}
