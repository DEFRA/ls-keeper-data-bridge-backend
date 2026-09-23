using System.Globalization;
using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.EtlPipeline.Optimise;

/// <summary>Converts one source string to its resolved <see cref="ColumnDataType"/>.
///
/// Strict by design: a value that does not parse throws <see cref="FormatException"/>, which the
/// caller wraps in <see cref="Stages.SourceFileConversionException"/> with the file, column and
/// record position. Null and empty stay null - the source writes empty fields as null and a typed
/// column should not reinterpret that.</summary>
public static class ColumnValueParser
{
    private static readonly string[] s_dateFormats = ["yyyy-MM-dd", "yyyyMMdd"];

    private static readonly string[] s_timestampFormats =
    [
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.FFFFFFF",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd HH:mm:ss.FFFFFFF",
        "yyyyMMddHHmmss",
        "yyyy-MM-dd"
    ];

    public static object? Parse(string? value, ColumnDataType target, byte decimalPrecision, byte decimalScale)
        => target switch
        {
            ColumnDataType.String => value,
            ColumnDataType.Int64 => ParseInt64(value),
            ColumnDataType.Double => ParseDouble(value),
            ColumnDataType.Boolean => ParseBoolean(value),
            ColumnDataType.Date => ParseDate(value),
            ColumnDataType.Timestamp => ParseTimestamp(value),
            ColumnDataType.Decimal => ParseDecimal(value, decimalPrecision, decimalScale),
            _ => throw new InvalidOperationException($"Unknown column data type '{target}'.")
        };

    public static long? ParseInt64(string? value)
    {
        if (value is null or { Length: 0 }) return null;

        return long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : throw Failure(value);
    }

    public static double? ParseDouble(string? value)
    {
        if (value is null or { Length: 0 }) return null;

        return double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : throw Failure(value);
    }

    public static bool? ParseBoolean(string? value)
    {
        if (value is null or { Length: 0 }) return null;

        if (value.Equals("true", StringComparison.OrdinalIgnoreCase) || value == "1") return true;

        if (value.Equals("false", StringComparison.OrdinalIgnoreCase) || value == "0") return false;

        throw Failure(value);
    }

    public static DateOnly? ParseDate(string? value)
    {
        if (value is null or { Length: 0 }) return null;

        return DateOnly.TryParseExact(value, s_dateFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed)
            ? parsed
            : throw Failure(value);
    }

    public static DateTime? ParseTimestamp(string? value)
    {
        if (value is null or { Length: 0 }) return null;

        return DateTime.TryParseExact(value, s_timestampFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed)
            ? parsed
            : throw Failure(value);
    }

    /// <summary>A decimal must fit the declared precision and scale as well as parse: parquet
    /// carries the digits and a reader applies (p, s) on the way out, so a value that overflows
    /// them is silently wrong downstream rather than rejected here.</summary>
    public static decimal? ParseDecimal(string? value, byte precision, byte scale)
    {
        if (value is null or { Length: 0 }) return null;

        if (!decimal.TryParse(value, NumberStyles.Number, CultureInfo.InvariantCulture, out var parsed)) throw Failure(value);

        if (decimal.Round(parsed, scale) != parsed) throw Failure(value);

        var integerDigits = decimal.Truncate(parsed).ToString(CultureInfo.InvariantCulture).TrimStart('-').Length;

        if (integerDigits > precision - scale) throw Failure(value);

        return parsed;
    }

    private static FormatException Failure(string value)
        => new($"Value '{value}' is not convertible.");
}
