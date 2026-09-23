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
}
