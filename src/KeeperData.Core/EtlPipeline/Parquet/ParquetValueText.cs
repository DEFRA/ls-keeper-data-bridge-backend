using System;
using System.Collections.Generic;
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

    private static readonly IReadOnlyDictionary<Type, Func<string, object?>> Parsers =
        new Dictionary<Type, Func<string, object?>>(TypeComparer.Instance)
        {
            [typeof(string)] = s => s,
            [typeof(long)] = s => long.Parse(s, CultureInfo.InvariantCulture),
            [typeof(int)] = s => int.Parse(s, CultureInfo.InvariantCulture),
            [typeof(short)] = s => short.Parse(s, CultureInfo.InvariantCulture),
            [typeof(byte)] = s => byte.Parse(s, CultureInfo.InvariantCulture),
            [typeof(double)] = s => double.Parse(s, NumberStyles.Float, CultureInfo.InvariantCulture),
            [typeof(float)] = s => float.Parse(s, NumberStyles.Float, CultureInfo.InvariantCulture),
            [typeof(decimal)] = s => decimal.Parse(s, CultureInfo.InvariantCulture),
            [typeof(bool)] = s => bool.Parse(s),
            [typeof(DateTime)] = s => DateTime.ParseExact(s, "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            [typeof(DateTimeOffset)] = s => DateTimeOffset.ParseExact(s, "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            [typeof(DateOnly)] = s => DateOnly.ParseExact(s, "O", CultureInfo.InvariantCulture),
            [typeof(TimeSpan)] = s => TimeSpan.ParseExact(s, "c", CultureInfo.InvariantCulture),
            [typeof(Guid)] = s => Guid.Parse(s),
            [typeof(byte[])] = s => Convert.FromBase64String(s),
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

        if (Parsers.TryGetValue(target, out var parser))
        {
            return parser(text);
        }

        throw new InvalidOperationException($"No canonical text form for parquet column type '{target}'.");
    }

    /// <summary>Parse without throwing - false when the text is not a value of the type. Used to
    /// test whether held canonical strings can adopt a column's new type before the schema commits
    /// to it. A type with no parser still throws: that is a programming error, not bad data.</summary>
    public static bool TryParse(Type clrType, string? text, out object? parsed)
    {
        try
        {
            parsed = Parse(clrType, text);
            return true;
        }
        catch (Exception ex) when (ex is FormatException or OverflowException)
        {
            parsed = null;
            return false;
        }
    }

    // Dictionary<Type,...> uses reference equality by default but System.Type behaves like value for our keys.
    private sealed class TypeComparer : IEqualityComparer<Type>
    {
        public static readonly TypeComparer Instance = new();
        public bool Equals(Type? x, Type? y) => x == y;
        public int GetHashCode(Type obj) => obj.GetHashCode();
    }
}
