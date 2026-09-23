using System.Globalization;

namespace KeeperData.Core.EtlPipeline.Parquet;

/// <summary>The canonical, lossless, culture-invariant text form of a typed parquet value.
///
/// The merge holds rows as strings so keying, ordering and schema drift work on one
/// representation; this is the conversion that makes that lossless at the edges - Format on the
/// way in, Parse on the way out, and the pair must round-trip for every supported CLR type.</summary>
public static class ParquetValueText
{
    public static string? Format(object? value) => value switch
    {
        null => null,
        string text => text,
        bool boolean => boolean.ToString(),
        DateTime dateTime => dateTime.ToString("O", CultureInfo.InvariantCulture),
        DateTimeOffset dateTimeOffset => dateTimeOffset.ToString("O", CultureInfo.InvariantCulture),
        DateOnly date => date.ToString("O", CultureInfo.InvariantCulture),
        TimeSpan timeSpan => timeSpan.ToString("c", CultureInfo.InvariantCulture),
        Guid guid => guid.ToString("D"),
        double number => number.ToString("R", CultureInfo.InvariantCulture),
        float number => number.ToString("R", CultureInfo.InvariantCulture),
        decimal number => number.ToString(CultureInfo.InvariantCulture),
        byte[] bytes => Convert.ToBase64String(bytes),
        IConvertible convertible => convertible.ToString(CultureInfo.InvariantCulture),
        _ => value.ToString()
    };

    /// <summary>The inverse of <see cref="Format"/>: parse a canonical string back to the CLR type
    /// the field declares. <paramref name="clrType"/> may be a Nullable&lt;T&gt;.</summary>
    public static object? Parse(Type clrType, string? text)
    {
        if (text is null)
        {
            return null;
        }

        var target = Nullable.GetUnderlyingType(clrType) ?? clrType;

        if (target == typeof(string)) return text;
        if (target == typeof(long)) return long.Parse(text, CultureInfo.InvariantCulture);
        if (target == typeof(int)) return int.Parse(text, CultureInfo.InvariantCulture);
        if (target == typeof(short)) return short.Parse(text, CultureInfo.InvariantCulture);
        if (target == typeof(byte)) return byte.Parse(text, CultureInfo.InvariantCulture);
        if (target == typeof(double)) return double.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture);
        if (target == typeof(float)) return float.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture);
        if (target == typeof(decimal)) return decimal.Parse(text, CultureInfo.InvariantCulture);
        if (target == typeof(bool)) return bool.Parse(text);
        if (target == typeof(DateTime)) return DateTime.ParseExact(text, "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        if (target == typeof(DateTimeOffset)) return DateTimeOffset.ParseExact(text, "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        if (target == typeof(DateOnly)) return DateOnly.ParseExact(text, "O", CultureInfo.InvariantCulture);
        if (target == typeof(TimeSpan)) return TimeSpan.ParseExact(text, "c", CultureInfo.InvariantCulture);
        if (target == typeof(Guid)) return Guid.Parse(text);
        if (target == typeof(byte[])) return Convert.FromBase64String(text);

        throw new InvalidOperationException($"No canonical text form for parquet column type '{target}'.");
    }
}
