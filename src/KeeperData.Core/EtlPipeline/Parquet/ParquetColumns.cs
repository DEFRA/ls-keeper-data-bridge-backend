using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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
    private static readonly IReadOnlyDictionary<Type, Func<ParquetRowGroupReader, DataField, int, CancellationToken, Task<Array>>> ReadHandlers;
    private static readonly IReadOnlyDictionary<Type, Func<ParquetRowGroupWriter, DataField, Array, CancellationToken, Task>> WriteHandlers;

    static ParquetColumns()
    {
        ReadHandlers = new Dictionary<Type, Func<ParquetRowGroupReader, DataField, int, CancellationToken, Task<Array>>>
        {
            [typeof(string)] = (r,f,rows,ct) => ReadStringsAsync(r,f,rows,ct),
            [typeof(ReadOnlyMemory<char>)] = (r,f,rows,ct) => ReadStringsAsync(r,f,rows,ct),
            [typeof(byte[])] = (r,f,rows,ct) => ReadBytesAsync(r,f,rows,ct),
            [typeof(ReadOnlyMemory<byte>)] = (r,f,rows,ct) => ReadBytesAsync(r,f,rows,ct),

            [typeof(bool)] = (r,f,rows,ct) => ReadTypedAsync<bool>(r,f,rows,ct),
            [typeof(long)] = (r,f,rows,ct) => ReadTypedAsync<long>(r,f,rows,ct),
            [typeof(int)] = (r,f,rows,ct) => ReadTypedAsync<int>(r,f,rows,ct),
            [typeof(short)] = (r,f,rows,ct) => ReadTypedAsync<short>(r,f,rows,ct),
            [typeof(byte)] = (r,f,rows,ct) => ReadTypedAsync<byte>(r,f,rows,ct),
            [typeof(double)] = (r,f,rows,ct) => ReadTypedAsync<double>(r,f,rows,ct),
            [typeof(float)] = (r,f,rows,ct) => ReadTypedAsync<float>(r,f,rows,ct),
            [typeof(decimal)] = (r,f,rows,ct) => ReadTypedAsync<decimal>(r,f,rows,ct),
            [typeof(DateTime)] = (r,f,rows,ct) => ReadTypedAsync<DateTime>(r,f,rows,ct),
            [typeof(DateTimeOffset)] = (r,f,rows,ct) => ReadTypedAsync<DateTimeOffset>(r,f,rows,ct),
            [typeof(DateOnly)] = (r,f,rows,ct) => ReadTypedAsync<DateOnly>(r,f,rows,ct),
            [typeof(TimeSpan)] = (r,f,rows,ct) => ReadTypedAsync<TimeSpan>(r,f,rows,ct),
            [typeof(Guid)] = (r,f,rows,ct) => ReadTypedAsync<Guid>(r,f,rows,ct),
        };

        WriteHandlers = new Dictionary<Type, Func<ParquetRowGroupWriter, DataField, Array, CancellationToken, Task>>
        {
            [typeof(string)] = (w,f,v,ct) => w.WriteAsync(f, (IReadOnlyCollection<string?>)(string?[])v),
            [typeof(ReadOnlyMemory<char>)] = (w,f,v,ct) => w.WriteAsync(f, (IReadOnlyCollection<string?>)(string?[])v),
            [typeof(byte[])] = (w,f,v,ct) => w.WriteAsync(f, (IReadOnlyCollection<byte[]>)(byte[][])v),
            [typeof(ReadOnlyMemory<byte>)] = (w,f,v,ct) => w.WriteAsync(f, (IReadOnlyCollection<byte[]>)(byte[][])v),

            [typeof(bool)] = (w,f,v,ct) => WriteTypedAsync<bool>(w,f,v,ct),
            [typeof(long)] = (w,f,v,ct) => WriteTypedAsync<long>(w,f,v,ct),
            [typeof(int)] = (w,f,v,ct) => WriteTypedAsync<int>(w,f,v,ct),
            [typeof(short)] = (w,f,v,ct) => WriteTypedAsync<short>(w,f,v,ct),
            [typeof(byte)] = (w,f,v,ct) => WriteTypedAsync<byte>(w,f,v,ct),
            [typeof(double)] = (w,f,v,ct) => WriteTypedAsync<double>(w,f,v,ct),
            [typeof(float)] = (w,f,v,ct) => WriteTypedAsync<float>(w,f,v,ct),
            [typeof(decimal)] = (w,f,v,ct) => WriteTypedAsync<decimal>(w,f,v,ct),
            [typeof(DateTime)] = (w,f,v,ct) => WriteTypedAsync<DateTime>(w,f,v,ct),
            [typeof(DateTimeOffset)] = (w,f,v,ct) => WriteTypedAsync<DateTimeOffset>(w,f,v,ct),
            [typeof(DateOnly)] = (w,f,v,ct) => WriteTypedAsync<DateOnly>(w,f,v,ct),
            [typeof(TimeSpan)] = (w,f,v,ct) => WriteTypedAsync<TimeSpan>(w,f,v,ct),
            [typeof(Guid)] = (w,f,v,ct) => WriteTypedAsync<Guid>(w,f,v,ct),
        };
    }

    /// <summary>Reads one column's values into an array of the field's CLR element type.</summary>
    public static Task<Array> ReadAsync(ParquetRowGroupReader reader, DataField field, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(field);

        var rows = (int)reader.RowCount;
        var clr = Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType;

        if (ReadHandlers.TryGetValue(clr, out var handler))
            return handler(reader, field, rows, cancellationToken);

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

        if (WriteHandlers.TryGetValue(clr, out var handler))
            return handler(writer, field, values, cancellationToken);

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
