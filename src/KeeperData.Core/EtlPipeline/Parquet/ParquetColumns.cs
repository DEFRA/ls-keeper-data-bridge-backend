using Parquet;
using Parquet.Schema;

namespace KeeperData.Core.EtlPipeline.Parquet;

/// <summary>Row-group IO for columns whose CLR type is only known at runtime.
///
/// Parquet.Net's row-group API is generic - the array you read into or write from must match the
/// field's declared type - so both directions switch on <see cref="DataField.ClrType"/> to a typed
/// helper. A column of a type outside the supported set fails loudly rather than being read wrong.</summary>
public static class ParquetColumns
{
    /// <summary>Reads one column's values into an array of the field's CLR element type.</summary>
    public static Task<Array> ReadAsync(ParquetRowGroupReader reader, DataField field, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(field);

        var rows = (int)reader.RowCount;
        var clr = Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType;

        // Parquet.Net declares strings as ReadOnlyMemory<char> and byte arrays as ReadOnlyMemory<byte>.
        if (clr == typeof(string) || clr == typeof(ReadOnlyMemory<char>)) return ReadStringsAsync(reader, field, rows, cancellationToken);
        if (clr == typeof(byte[]) || clr == typeof(ReadOnlyMemory<byte>)) return ReadBytesAsync(reader, field, rows, cancellationToken);
        if (clr == typeof(bool)) return ReadTypedAsync<bool>(reader, field, rows, cancellationToken);
        if (clr == typeof(long)) return ReadTypedAsync<long>(reader, field, rows, cancellationToken);
        if (clr == typeof(int)) return ReadTypedAsync<int>(reader, field, rows, cancellationToken);
        if (clr == typeof(short)) return ReadTypedAsync<short>(reader, field, rows, cancellationToken);
        if (clr == typeof(byte)) return ReadTypedAsync<byte>(reader, field, rows, cancellationToken);
        if (clr == typeof(double)) return ReadTypedAsync<double>(reader, field, rows, cancellationToken);
        if (clr == typeof(float)) return ReadTypedAsync<float>(reader, field, rows, cancellationToken);
        if (clr == typeof(decimal)) return ReadTypedAsync<decimal>(reader, field, rows, cancellationToken);
        if (clr == typeof(DateTime)) return ReadTypedAsync<DateTime>(reader, field, rows, cancellationToken);
        if (clr == typeof(DateTimeOffset)) return ReadTypedAsync<DateTimeOffset>(reader, field, rows, cancellationToken);
        if (clr == typeof(DateOnly)) return ReadTypedAsync<DateOnly>(reader, field, rows, cancellationToken);
        if (clr == typeof(TimeSpan)) return ReadTypedAsync<TimeSpan>(reader, field, rows, cancellationToken);
        if (clr == typeof(Guid)) return ReadTypedAsync<Guid>(reader, field, rows, cancellationToken);

        throw Unsupported(field);
    }

    /// <summary>The CLR type an array of the field's values is held in - the element type of what
    /// <see cref="ReadAsync"/> produces and <see cref="WriteAsync"/> consumes. Text and binary fields
    /// report ReadOnlyMemory spans on <see cref="DataField.ClrType"/>, but their column IO takes
    /// <c>string</c> and <c>byte[]</c> collections, so those map back.</summary>
    public static Type ElementType(DataField field)
    {
        var clr = Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType;

        return clr == typeof(ReadOnlyMemory<char>) ? typeof(string)
            : clr == typeof(ReadOnlyMemory<byte>) ? typeof(byte[])
            : field.ClrNullableIfHasNullsType;
    }

    /// <summary>Reads one column's values as their canonical text forms - a string column's values
    /// pass through unchanged, a typed column's go through <see cref="ParquetValueText.Format"/>.</summary>
    public static async Task<string?[]> ReadAsStringsAsync(ParquetRowGroupReader reader, DataField field, CancellationToken cancellationToken)
    {
        var data = await ReadAsync(reader, field, cancellationToken);
        var strings = new string?[data.Length];

        for (var row = 0; row < data.Length; row++)
        {
            strings[row] = ParquetValueText.Format(data.GetValue(row));
        }

        return strings;
    }

    /// <summary>Writes one column's values from an array of the field's CLR element type.</summary>
    public static Task WriteAsync(ParquetRowGroupWriter writer, DataField field, Array values, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(field);
        ArgumentNullException.ThrowIfNull(values);

        var clr = Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType;

        // Parquet.Net declares strings as ReadOnlyMemory<char> and byte arrays as ReadOnlyMemory<byte>.
        if (clr == typeof(string) || clr == typeof(ReadOnlyMemory<char>)) return writer.WriteAsync(field, (IReadOnlyCollection<string?>)(string?[])values);
        if (clr == typeof(byte[]) || clr == typeof(ReadOnlyMemory<byte>)) return writer.WriteAsync(field, (IReadOnlyCollection<byte[]>)(byte[][])values);
        if (clr == typeof(bool)) return WriteTypedAsync<bool>(writer, field, values, cancellationToken);
        if (clr == typeof(long)) return WriteTypedAsync<long>(writer, field, values, cancellationToken);
        if (clr == typeof(int)) return WriteTypedAsync<int>(writer, field, values, cancellationToken);
        if (clr == typeof(short)) return WriteTypedAsync<short>(writer, field, values, cancellationToken);
        if (clr == typeof(byte)) return WriteTypedAsync<byte>(writer, field, values, cancellationToken);
        if (clr == typeof(double)) return WriteTypedAsync<double>(writer, field, values, cancellationToken);
        if (clr == typeof(float)) return WriteTypedAsync<float>(writer, field, values, cancellationToken);
        if (clr == typeof(decimal)) return WriteTypedAsync<decimal>(writer, field, values, cancellationToken);
        if (clr == typeof(DateTime)) return WriteTypedAsync<DateTime>(writer, field, values, cancellationToken);
        if (clr == typeof(DateTimeOffset)) return WriteTypedAsync<DateTimeOffset>(writer, field, values, cancellationToken);
        if (clr == typeof(DateOnly)) return WriteTypedAsync<DateOnly>(writer, field, values, cancellationToken);
        if (clr == typeof(TimeSpan)) return WriteTypedAsync<TimeSpan>(writer, field, values, cancellationToken);
        if (clr == typeof(Guid)) return WriteTypedAsync<Guid>(writer, field, values, cancellationToken);

        throw Unsupported(field);
    }

    private static async Task<Array> ReadStringsAsync(ParquetRowGroupReader reader, DataField field, int rows, CancellationToken cancellationToken)
    {
        var buffer = new string?[rows];
        await reader.ReadAsync(field, buffer.AsMemory(), cancellationToken: cancellationToken);
        return buffer;
    }

    private static async Task<Array> ReadBytesAsync(ParquetRowGroupReader reader, DataField field, int rows, CancellationToken cancellationToken)
    {
        var buffer = new byte[]?[rows];
        await reader.ReadAsync(field, buffer.AsMemory(), cancellationToken: cancellationToken);
        return buffer;
    }

    private static async Task<Array> ReadTypedAsync<T>(ParquetRowGroupReader reader, DataField field, int rows, CancellationToken cancellationToken)
        where T : struct
    {
        if (field.IsNullable)
        {
            var buffer = new T?[rows];
            await reader.ReadAsync<T>(field, buffer.AsMemory(), cancellationToken: cancellationToken);
            return buffer;
        }

        var required = new T[rows];
        await reader.ReadAsync<T>(field, required.AsMemory(), cancellationToken: cancellationToken);
        return required;
    }

    private static Task WriteTypedAsync<T>(ParquetRowGroupWriter writer, DataField field, Array values, CancellationToken cancellationToken)
        where T : struct
        => field.IsNullable
            ? writer.WriteAsync<T>(field, ((T?[])values).AsMemory(), cancellationToken: cancellationToken)
            : writer.WriteAsync<T>(field, ((T[])values).AsMemory(), cancellationToken: cancellationToken);

    private static InvalidOperationException Unsupported(DataField field)
        => new($"Parquet column '{field.Name}' has unsupported CLR type '{field.ClrType}'.");
}
