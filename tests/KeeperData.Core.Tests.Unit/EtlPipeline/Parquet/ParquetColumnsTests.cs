using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using FluentAssertions;
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

