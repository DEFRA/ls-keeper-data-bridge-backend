using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace KeeperData.Core.Tests.Unit.EtlPipeline.Harness;

/// <summary>Builds and reads the small all-string Parquet files the snapshot tests trade in, written
/// in the pipe-separated shape the source files arrive in so a fixture reads like the ticket's.
///
///   ParquetFixture.From("CHANGE_TYPE|CPH|HOLDING_NAME", "I|01/001/0001|Old Farm")
/// </summary>
public static class ParquetFixture
{
    public static byte[] From(string header, params string[] rows)
    {
        var columns = header.Split('|');
        var values = columns.Select(_ => new List<string?>()).ToArray();

        foreach (var row in rows)
        {
            var cells = row.Split('|');

            for (var column = 0; column < columns.Length; column++)
            {
                values[column].Add(column < cells.Length ? cells[column] : null);
            }
        }

        var fields = columns.Select(column => new DataField<string>(column)).ToArray();
        var buffer = new MemoryStream();

        using (var writer = ParquetWriter.CreateAsync(new ParquetSchema(fields), buffer).GetAwaiter().GetResult())
        {
            using var rowGroup = writer.CreateRowGroup();

            for (var column = 0; column < fields.Length; column++)
            {
                rowGroup.WriteColumnAsync(new DataColumn(fields[column], values[column].ToArray())).GetAwaiter().GetResult();
            }
        }

        return buffer.ToArray();
    }

    /// <summary>The file read back as pipe-separated lines, header first, so an assertion can be
    /// written the same way as the fixture.</summary>
    public static IReadOnlyList<string> ToLines(byte[] content)
    {
        using var reader = ParquetReader.CreateAsync(new MemoryStream(content)).GetAwaiter().GetResult();

        var fields = reader.Schema.GetDataFields();
        var lines = new List<string> { string.Join('|', fields.Select(field => field.Name)) };

        for (var group = 0; group < reader.RowGroupCount; group++)
        {
            using var rowGroup = reader.OpenRowGroupReader(group);

            var columns = fields
                .Select(field => rowGroup.ReadColumnAsync(field).GetAwaiter().GetResult().Data)
                .ToArray();

            for (var row = 0; row < (columns.Length == 0 ? 0 : columns[0].Length); row++)
            {
                lines.Add(string.Join('|', columns.Select(column => column.GetValue(row)?.ToString() ?? string.Empty)));
            }
        }

        return lines;
    }
}
