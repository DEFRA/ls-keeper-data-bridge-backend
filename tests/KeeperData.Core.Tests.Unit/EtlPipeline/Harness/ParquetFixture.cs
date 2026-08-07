using Parquet;
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

        WriteParquet(fields, values, buffer);

        return buffer.ToArray();
    }

    private static void WriteParquet(DataField<string>[] fields, List<string?>[] values, MemoryStream buffer)
    {
        var task = WriteParquetAsync(fields, values, buffer);
        task.GetAwaiter().GetResult();
    }

    private static async Task WriteParquetAsync(DataField<string>[] fields, List<string?>[] values, MemoryStream buffer)
    {
        await using var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), buffer);
        using var rowGroup = writer.CreateRowGroup();

        for (var column = 0; column < fields.Length; column++)
        {
            await rowGroup.WriteAsync(fields[column], (IReadOnlyCollection<string?>)values[column]);
        }
    }

    /// <summary>The file read back as pipe-separated lines, header first, so an assertion can be
    /// written the same way as the fixture.</summary>
    public static IReadOnlyList<string> ToLines(byte[] content)
    {
        return ReadLinesAsync(content).GetAwaiter().GetResult();
    }

    private static async Task<IReadOnlyList<string>> ReadLinesAsync(byte[] content)
    {
        await using var reader = await ParquetReader.CreateAsync(new MemoryStream(content));

        var fields = reader.Schema.GetDataFields();
        var lines = new List<string> { string.Join('|', fields.Select(field => field.Name)) };

        for (var group = 0; group < reader.RowGroupCount; group++)
        {
            using var rowGroup = reader.OpenRowGroupReader(group);

            var rowCount = (int)rowGroup.RowCount;
            var columns = new string?[fields.Length][];

            for (var col = 0; col < fields.Length; col++)
            {
                var buf = new string?[rowCount];
                await rowGroup.ReadAsync(fields[col], buf.AsMemory());
                columns[col] = buf;
            }

            for (var row = 0; row < rowCount; row++)
            {
                lines.Add(string.Join('|', columns.Select(column => column?[row] ?? string.Empty)));
            }
        }

        return lines;
    }
}
