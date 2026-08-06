using Parquet;
using Parquet.Schema;

namespace KeeperData.Core.EtlPipeline.Snapshots;

public sealed partial class ParquetDeltaMergeEngine
{
    /// <summary>One Parquet file read into rows, so the merge works one file at a time rather than
    /// column-at-a-time across files.</summary>
    private sealed class ParquetTable
    {
        private readonly Dictionary<string, int> _indexByName = new(StringComparer.OrdinalIgnoreCase);

        private ParquetTable(DataField[] fields, List<string?[]> rows)
        {
            Fields = fields;
            Rows = rows;

            for (var index = 0; index < fields.Length; index++)
            {
                _indexByName.TryAdd(fields[index].Name, index);
            }
        }

        public DataField[] Fields { get; }

        public List<string?[]> Rows { get; }

        public int IndexOf(string name) => _indexByName.TryGetValue(name, out var index) ? index : -1;

        public static async Task<ParquetTable> ReadAsync(Stream stream, string key, CancellationToken cancellationToken)
        {
            await using var seekable = await AsSeekableAsync(stream, cancellationToken);
            await using var reader = await ParquetReader.CreateAsync(seekable, cancellationToken: cancellationToken);

            var fields = reader.Schema.GetDataFields();
            var rows = await ReadRowsAsync(reader, fields, cancellationToken);

            return fields.Length == 0 && rows.Count == 0
                ? throw new InvalidOperationException($"'{key}' carries no columns")
                : new ParquetTable(fields, rows);
        }

        private static async Task<List<string?[]>> ReadRowsAsync(ParquetReader reader, DataField[] fields, CancellationToken cancellationToken)
        {
            var rows = new List<string?[]>();

            for (var group = 0; group < reader.RowGroupCount; group++)
            {
                using var rowGroup = reader.OpenRowGroupReader(group);
                var columns = await ReadColumnsAsync(rowGroup, fields, cancellationToken);
                AppendRows(rows, columns, fields.Length);
            }

            return rows;
        }

        private static async Task<string[][]> ReadColumnsAsync(ParquetRowGroupReader rowGroup, DataField[] fields, CancellationToken cancellationToken)
        {
            var rowCount = (int)rowGroup.RowCount;
            var columns = new string[fields.Length][];

            for (var column = 0; column < fields.Length; column++)
            {
                var buf = new string[rowCount];
                await rowGroup.ReadAsync(fields[column], buf.AsMemory(), cancellationToken: cancellationToken);
                columns[column] = buf;
            }

            return columns;
        }

        private static void AppendRows(List<string?[]> rows, string[][] columns, int fieldCount)
        {
            var count = columns.Length == 0 ? 0 : columns[0].Length;

            for (var row = 0; row < count; row++)
            {
                var values = new string?[fieldCount];

                for (var column = 0; column < fieldCount; column++)
                {
                    values[column] = columns[column][row];
                }

                rows.Add(values);
            }
        }

        /// <summary>Parquet is read back to front, so a forward-only stream (an object download) is
        /// buffered first.</summary>
        private static async Task<Stream> AsSeekableAsync(Stream stream, CancellationToken cancellationToken)
        {
            if (stream.CanSeek)
            {
                stream.Position = 0;
                return new NonDisposingStream(stream);
            }

            var buffer = new MemoryStream();
            await stream.CopyToAsync(buffer, cancellationToken);
            buffer.Position = 0;

            return buffer;
        }
    }
}
