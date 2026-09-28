using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Parquet;
using Parquet.Schema;
using Xunit;

namespace KeeperData.Core.Tests.Unit.EtlPipeline.Parquet;

public class ParquetColumnsTests
{
    [Fact]
    public void ReadHandlers_contains_expected_types()
    {
        var field = typeof(KeeperData.Core.EtlPipeline.Parquet.ParquetColumns);
        var fi = field.GetField("ReadHandlers", BindingFlags.NonPublic | BindingFlags.Static);
        fi.Should().NotBeNull();

        var dict = fi!.GetValue(null) as System.Collections.IDictionary;
        dict.Should().NotBeNull();

        var expected = new[]
        {
            typeof(string), typeof(ReadOnlyMemory<char>), typeof(byte[]), typeof(ReadOnlyMemory<byte>),
            typeof(bool), typeof(long), typeof(int), typeof(short), typeof(byte), typeof(double), typeof(float),
            typeof(decimal), typeof(DateTime), typeof(DateTimeOffset), typeof(DateOnly), typeof(TimeSpan), typeof(Guid)
        };

        foreach (var t in expected)
        {
            dict.Contains(t).Should().BeTrue($"ReadHandlers must include handler for {t}");
        }
    }

    [Fact]
    public void WriteHandlers_contains_expected_types()
    {
        var field = typeof(KeeperData.Core.EtlPipeline.Parquet.ParquetColumns);
        var fi = field.GetField("WriteHandlers", BindingFlags.NonPublic | BindingFlags.Static);
        fi.Should().NotBeNull();

        var dict = fi!.GetValue(null) as System.Collections.IDictionary;
        dict.Should().NotBeNull();

        var expected = new[]
        {
            typeof(string), typeof(ReadOnlyMemory<char>), typeof(byte[]), typeof(ReadOnlyMemory<byte>),
            typeof(bool), typeof(long), typeof(int), typeof(short), typeof(byte), typeof(double), typeof(float),
            typeof(decimal), typeof(DateTime), typeof(DateTimeOffset), typeof(DateOnly), typeof(TimeSpan), typeof(Guid)
        };

        foreach (var t in expected)
        {
            dict.Contains(t).Should().BeTrue($"WriteHandlers must include handler for {t}");
        }
    }

    [Fact]
    public void ElementType_maps_readonlymemory_char_to_string()
    {
        var field = new DataField("f", typeof(ReadOnlyMemory<char>));

        var et = KeeperData.Core.EtlPipeline.Parquet.ParquetColumns.ElementType(field);

        et.Should().Be(typeof(string));
    }

    [Fact]
    public void ElementType_maps_readonlymemory_byte_to_byte_array()
    {
        var field = new DataField("f", typeof(ReadOnlyMemory<byte>));

        var et = KeeperData.Core.EtlPipeline.Parquet.ParquetColumns.ElementType(field);

        et.Should().Be(typeof(byte[]));
    }

    [Fact]
    public void ElementType_returns_field_type_for_other_types()
    {
        var field = new DataField("f", typeof(int));

        var et = KeeperData.Core.EtlPipeline.Parquet.ParquetColumns.ElementType(field);

        et.Should().Be(field.ClrNullableIfHasNullsType);
    }

    [Fact]
    public void ReadHandlers_delegates_return_TaskArray()
    {
        var field = typeof(KeeperData.Core.EtlPipeline.Parquet.ParquetColumns);
        var fi = field.GetField("ReadHandlers", BindingFlags.NonPublic | BindingFlags.Static);
        fi.Should().NotBeNull();

        var dict = fi!.GetValue(null) as System.Collections.IDictionary;
        dict.Should().NotBeNull();

        foreach (System.Collections.DictionaryEntry e in dict!)
        {
            var del = e.Value as Delegate;
            del.Should().NotBeNull();
            del!.Method.ReturnType.Should().Be(typeof(Task<Array>));
        }
    }

    [Fact]
    public void WriteHandlers_delegates_return_Task()
    {
        var field = typeof(KeeperData.Core.EtlPipeline.Parquet.ParquetColumns);
        var fi = field.GetField("WriteHandlers", BindingFlags.NonPublic | BindingFlags.Static);
        fi.Should().NotBeNull();

        var dict = fi!.GetValue(null) as System.Collections.IDictionary;
        dict.Should().NotBeNull();

        foreach (System.Collections.DictionaryEntry e in dict!)
        {
            var del = e.Value as Delegate;
            del.Should().NotBeNull();
            del!.Method.ReturnType.Should().Be(typeof(Task));
        }
    }

    /// <summary>Every registered type round-trips through a real parquet file: written via
    /// <see cref="KeeperData.Core.EtlPipeline.Parquet.ParquetColumns.WriteAsync"/> (through the
    /// fixture's FromTyped) and read back via ReadAsync through ToLines' canonical text.
    /// (DateTimeOffset and TimeSpan are registered but Parquet.Net no longer writes them; DateOnly
    /// writes as a DATE but Parquet.Net reads the logical type back as DateTime.)</summary>
    [Fact]
    public void Every_supported_type_writes_and_reads_back()
    {
        var stamp = new DateTime(2026, 9, 22, 10, 30, 15, DateTimeKind.Utc);
        var columns = new (DataField Field, Array Values)[]
        {
            (new DataField<string>("txt"), new string?[] { "abc", null }),
            (new DataField<byte[]>("bin"), new byte[][] { [1, 2, 3], [4] }),
            (new DataField<bool>("flag"), new bool[] { true, false }),
            (new DataField<long>("big"), new long[] { 12, -7 }),
            (new DataField<int>("num"), new int[] { 3, 4 }),
            (new DataField<short>("small"), new short[] { 5, 6 }),
            (new DataField<byte>("octet"), new byte[] { 7, 8 }),
            (new DataField<double>("dbl"), new double[] { 1.5, 2.25 }),
            (new DataField<float>("flt"), new float[] { 1.5f, 2.25f }),
            (new DataField<decimal>("dec"), new decimal[] { 1.5m, 2.25m }),
            (new DataField<DateTime>("ts"), new DateTime[] { stamp, stamp.AddDays(1) }),
            (new DataField<DateOnly>("dt"), new DateOnly[] { new(2026, 9, 22), new(2026, 9, 23) }),
            (new DataField<Guid>("id"), new Guid[] { new("11111111-1111-1111-1111-111111111111"), new("22222222-2222-2222-2222-222222222222") }),
        };

        var lines = KeeperData.Core.Tests.Unit.EtlPipeline.Harness.ParquetFixture.ToLines(
            KeeperData.Core.Tests.Unit.EtlPipeline.Harness.ParquetFixture.FromTyped(columns));

        lines.Should().HaveCount(3);
        lines[1].Split('|').Should().Equal(
            "abc", Convert.ToBase64String(new byte[] { 1, 2, 3 }), "True", "12", "3", "5", "7",
            "1.5", "1.5", "1.5", "2026-09-22T10:30:15.0000000", "2026-09-22T00:00:00.0000000Z",
            "11111111-1111-1111-1111-111111111111");
    }

    /// <summary>Nullable typed columns take the nullable branch of the read/write helpers; the
    /// nulls must come back as nulls, not defaults.</summary>
    [Fact]
    public void Nullable_typed_columns_round_trip_their_nulls()
    {
        var columns = new (DataField Field, Array Values)[]
        {
            (new DataField<long?>("big"), new long?[] { 12, null, -7 }),
            (new DataField<bool?>("flag"), new bool?[] { null, true, null }),
            (new DataField<DateOnly?>("dt"), new DateOnly?[] { new(2026, 9, 22), null, new(2026, 9, 24) }),
        };

        var lines = KeeperData.Core.Tests.Unit.EtlPipeline.Harness.ParquetFixture.ToLines(
            KeeperData.Core.Tests.Unit.EtlPipeline.Harness.ParquetFixture.FromTyped(columns));

        lines.Should().HaveCount(4);
        lines[2].Should().Be("|True|");
        lines[3].Should().Be("-7||2026-09-24T00:00:00.0000000Z");
    }

    /// <summary>Parquet.Net keeps CLR types outside the dispatch set - uint, sbyte, ulong - and a
    /// column of one must fail loudly rather than be read wrong.</summary>
    [Fact]
    public async Task Reading_a_column_of_an_unsupported_type_fails_loudly()
    {
        var field = new DataField("u", typeof(uint));
        var buffer = new MemoryStream();

        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(field), buffer))
        {
            using var rowGroup = writer.CreateRowGroup();
            await rowGroup.WriteAsync<uint>(field, new uint[] { 1, 2 });
        }

        buffer.Position = 0;
        await using var reader = await ParquetReader.CreateAsync(buffer);
        using var group = reader.OpenRowGroupReader(0);
        var stored = reader.Schema.GetDataFields().Single(f => f.Name == "u");

        var read = async () => await KeeperData.Core.EtlPipeline.Parquet.ParquetColumns.ReadAsync(group, stored, CancellationToken.None);

        await read.Should().ThrowAsync<InvalidOperationException>().WithMessage("*unsupported CLR type*");
    }

    [Fact]
    public async Task Writing_a_column_of_an_unsupported_type_fails_loudly()
    {
        var field = new DataField("u", typeof(uint));
        var buffer = new MemoryStream();

        await using var writer = await ParquetWriter.CreateAsync(new ParquetSchema(field), buffer);
        using var rowGroup = writer.CreateRowGroup();

        var write = async () => await KeeperData.Core.EtlPipeline.Parquet.ParquetColumns.WriteAsync(rowGroup, field, new uint[] { 1 }, CancellationToken.None);

        await write.Should().ThrowAsync<InvalidOperationException>().WithMessage("*unsupported CLR type*");
    }

    [Fact]
    public void Unsupported_returns_a_diagnostic_invalidoperationexception()
    {
        var t = typeof(KeeperData.Core.EtlPipeline.Parquet.ParquetColumns);
        var mi = t.GetMethod("Unsupported", BindingFlags.NonPublic | BindingFlags.Static);
        mi.Should().NotBeNull();

        // DataField constructor rejects unsupported CLR types (throws NotSupportedException),
        // so create an uninitialized instance and populate the required members via reflection.
        var dfType = typeof(DataField);
        #pragma warning disable SYSLIB0050
                var field = (DataField)System.Runtime.Serialization.FormatterServices.GetUninitializedObject(dfType);
        #pragma warning restore SYSLIB0050

        // Set Name (property or backing field)
        var nameProp = dfType.GetProperty("Name");
        if (nameProp != null && nameProp.SetMethod != null)
            nameProp.SetValue(field, "x");
        else
        {
            var nameField = dfType.GetField("<Name>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic) ?? dfType.GetField("name", BindingFlags.Instance | BindingFlags.NonPublic);
            if (nameField != null) nameField.SetValue(field, "x");
        }

        // Set ClrType (property or backing field) to an unsupported type so Unsupported() can be exercised
        var clrProp = dfType.GetProperty("ClrType");
        if (clrProp != null && clrProp.SetMethod != null)
            clrProp.SetValue(field, typeof(object));
        else
        {
            var clrField = dfType.GetField("<ClrType>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic)
                        ?? dfType.GetField("clrType", BindingFlags.Instance | BindingFlags.NonPublic)
                        ?? dfType.GetField("type", BindingFlags.Instance | BindingFlags.NonPublic);
            if (clrField != null) clrField.SetValue(field, typeof(object));
        }

        var ex = mi!.Invoke(null, new object[] { field }) as InvalidOperationException;
        ex.Should().NotBeNull();
        ex!.Message.Should().Contain("unsupported CLR type");
    }
}

