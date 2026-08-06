using KeeperData.Core.ETL.Impl;
using Parquet;
using Parquet.Schema;

namespace KeeperData.Core.EtlPipeline.Snapshots;

public sealed partial class ParquetDeltaMergeEngine
{
    /// <summary>The merged rows, held in insertion order so the output is deterministic.</summary>
    private sealed class MergeState(DataSetDefinition definition)
    {
        private readonly Dictionary<string, int> _indexByKey = new(StringComparer.Ordinal);
        private readonly List<string?[]> _rows = [];

        private DataField[]? _fields;

        public long RowCount => _rows.Count;

        public void SeedFrom(ParquetTable table, string key)
        {
            _fields = table.Fields.Where(field => !IsChangeType(field.Name)).ToArray();

            foreach (var row in table.Rows)
            {
                Upsert(Project(table, row, key), CompositeKey(table, row, key));
            }
        }

        public (long Upserted, long IgnoredDeletes, long Rejected) Apply(ParquetTable table, string key)
        {
            _fields ??= table.Fields.Where(field => !IsChangeType(field.Name)).ToArray();

            var changeTypeIndex = table.IndexOf(definition.ChangeTypeHeaderName);

            var upserted = 0L;
            var ignoredDeletes = 0L;
            var rejected = 0L;

            foreach (var row in table.Rows)
            {
                var changeType = changeTypeIndex < 0 ? ChangeType.Insert : row[changeTypeIndex];

                switch (changeType)
                {
                    case ChangeType.Insert:
                    case ChangeType.Update:
                        Upsert(Project(table, row, key), CompositeKey(table, row, key));
                        upserted++;
                        break;

                    // Delete processing is out of scope: the row is counted and left in the snapshot.
                    case ChangeType.Delete:
                        ignoredDeletes++;
                        break;

                    default:
                        rejected++;
                        break;
                }
            }

            return (upserted, ignoredDeletes, rejected);
        }

        public async Task WriteAsync(Stream output, CancellationToken cancellationToken)
        {
            var fields = _fields
                ?? throw new InvalidOperationException($"Nothing to write for dataset '{definition.Name}': no file supplied a schema");

            await using var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), output, cancellationToken: cancellationToken);
            using var rowGroup = writer.CreateRowGroup();

            for (var column = 0; column < fields.Length; column++)
            {
                var values = new string?[_rows.Count];
                for (var row = 0; row < _rows.Count; row++)
                {
                    values[row] = _rows[row][column];
                }

                await rowGroup.WriteAsync(fields[column], (IReadOnlyCollection<string?>)values);
            }
        }

        /// <summary>The row reduced to the output columns, in output column order.</summary>
        private string?[] Project(ParquetTable table, string?[] row, string key)
        {
            var fields = _fields!;
            var projected = new string?[fields.Length];

            for (var column = 0; column < fields.Length; column++)
            {
                var index = table.IndexOf(fields[column].Name);

                if (index < 0)
                {
                    throw new InvalidOperationException(
                        $"'{key}' has no column '{fields[column].Name}' expected by dataset '{definition.Name}'");
                }

                projected[column] = row[index];
            }

            return projected;
        }

        private void Upsert(string?[] row, string compositeKey)
        {
            if (_indexByKey.TryGetValue(compositeKey, out var existing))
            {
                _rows[existing] = row;
                return;
            }

            _indexByKey[compositeKey] = _rows.Count;
            _rows.Add(row);
        }

        private string CompositeKey(ParquetTable table, string?[] row, string key)
        {
            var parts = definition.PrimaryKeyHeaderNames.Select(name =>
            {
                var index = table.IndexOf(name);

                return index < 0
                    ? throw new InvalidOperationException(
                        $"'{key}' has no primary key column '{name}' for dataset '{definition.Name}'")
                    : row[index] ?? string.Empty;
            });

            return string.Join(EtlConstants.CompositeKeyDelimiter, parts);
        }

        private bool IsChangeType(string name)
            => string.Equals(name, definition.ChangeTypeHeaderName, StringComparison.OrdinalIgnoreCase);
    }
}
