using System.Globalization;
using System.Text.RegularExpressions;
using KeeperData.Core.ETL.Impl;

namespace KeeperData.Core.EtlPipeline.Optimise;

/// <summary>Chooses the parquet type a string column's values look like.
///
/// Every candidate starts alive and each sampled value prunes the ones it cannot be, so the result
/// holds for every value seen, not just most of them. The preference order matters only where
/// candidates overlap: digits are ints before they are booleans, and a bare date is never a
/// timestamp.
///
/// The checks are deliberately stricter than the parsers used at write time - detection would
/// rather leave a column a string than guess it into a lossy type. In particular an integer
/// pattern rejects leading zeros ("007" is a code, not seven), a too-long integer never degrades
/// to a double (20 digits of a reference number must not become 1E+19), and no amount of sampling
/// produces Decimal - that type is only reachable through an explicit ColumnTypes entry.</summary>
public static class ColumnTypeDetector
{
    private static readonly TimeSpan s_regexTimeout = TimeSpan.FromMilliseconds(200);

    // No leading zeros and no leading '+': those strings carry information an Int64 cannot hold.
    private static readonly Regex s_int64 = new(@"^-?(0|[1-9][0-9]*)$", RegexOptions.Compiled | RegexOptions.CultureInvariant, s_regexTimeout);

    // Stricter than double.TryParse: no whitespace, no thousands separators, no NaN or Infinity,
    // and the integer part obeys the same no-leading-zero rule as Int64 - "007" is a code, and a
    // code that parses as a double still gets rewritten to 7.
    private static readonly Regex s_double = new(@"^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$", RegexOptions.Compiled | RegexOptions.CultureInvariant, s_regexTimeout);

    private static readonly string[] s_timestampFormats =
    [
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.FFFFFFF",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd HH:mm:ss.FFFFFFF"
    ];

    // The order candidates are preferred in when more than one survives the sample.
    private static readonly ColumnDataType[] s_preference =
    [
        ColumnDataType.Int64,
        ColumnDataType.Double,
        ColumnDataType.Boolean,
        ColumnDataType.Date,
        ColumnDataType.Timestamp
    ];

    /// <summary>The type every sampled value can be. A column that is entirely null or absent from
    /// the sample stays a string.</summary>
    public static ColumnDataType Detect(IEnumerable<string?> values)
    {
        var candidates = new HashSet<ColumnDataType>(s_preference);
        var seen = false;

        foreach (var value in values)
        {
            if (value is null or { Length: 0 }) continue;

            seen = true;
            candidates.IntersectWith(Classify(value));

            if (candidates.Count == 0) return ColumnDataType.String;
        }

        return !seen
            ? ColumnDataType.String
            : s_preference.FirstOrDefault(candidates.Contains, ColumnDataType.String);
    }

    /// <summary>Parse a value against the timestamp shapes the detector accepts. Exposed so a
    /// caller testing a held string can try the same source forms rather than only the canonical
    /// one.</summary>
    public static bool TryParseTimestamp(string value, out DateTime timestamp)
        => DateTime.TryParseExact(value, s_timestampFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out timestamp);

    /// <summary>Parse a value against the bare date shape the detector accepts.</summary>
    public static bool TryParseDate(string value, out DateOnly date)
        => DateOnly.TryParseExact(value, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out date);

    /// <summary>
    /// The types a single value is compatible with.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    private static IEnumerable<ColumnDataType> Classify(string value)
    {
        var intLike = s_int64.IsMatch(value);
        if (intLike && long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
        {
            yield return ColumnDataType.Int64;
            yield return ColumnDataType.Double;
        }
        else if (!intLike && s_double.IsMatch(value))
        {
            yield return ColumnDataType.Double;
        }

        if (IsBoolean(value)) yield return ColumnDataType.Boolean;


        if (DateOnly.TryParseExact(value, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out _)) 
            yield return ColumnDataType.Date;

        if (DateTime.TryParseExact(value, s_timestampFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out _)) 
            yield return ColumnDataType.Timestamp;

    }

    private static bool IsBoolean(string value)
        => value.Equals("true", StringComparison.OrdinalIgnoreCase)
            || value.Equals("false", StringComparison.OrdinalIgnoreCase);
}
