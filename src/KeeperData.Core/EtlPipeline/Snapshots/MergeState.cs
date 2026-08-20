using KeeperData.Core.ETL.Impl;
using Parquet;
using Parquet.Schema;

namespace KeeperData.Core.EtlPipeline.Snapshots;

public sealed partial class ParquetDeltaMergeEngine
{
    /// <summary>The merged rows, held in insertion order so the output is deterministic.
    ///
    /// The output schema is the union of the columns every file supplies, because a source extract can
    /// gain or lose a column between files and neither should stop the merge. A column a file does not
    /// carry is null for the rows that file supplies; a column a file introduces is appended and null
    /// for the rows already held. Both are reported as <see cref="SchemaDrift"/> so the caller can warn,
    /// since a column quietly appearing or disappearing is worth noticing even though it is tolerated.
    ///
    /// Note this nullifies per row, not per column: a row updated by a file that has dropped a column
    /// loses the value it previously held for it, while a row that file does not mention keeps its own.
    /// Primary keys are not treated this way - a file that cannot be keyed still fails the merge.</summary>
    private sealed class MergeState(DataSetDefinition definition)
    {
        private readonly Dictionary<string, int> _indexByKey = new(StringComparer.Ordinal);
        private readonly List<string?[]> _rows = [];

        private readonly List<DataField> _fields = [];
        private readonly Dictionary<string, int> _columnByName = new(StringComparer.OrdinalIgnoreCase);

        public long RowCount => _rows.Count;

        public SchemaDrift SeedFrom(ParquetTable table, string key)
        {
            var alignment = Align(table);

            foreach (var row in table.Rows)
            {
                Upsert(Project(alignment, row), CompositeKey(table, row, key));
            }

            return alignment.Drift;
        }

        public AppliedDelta Apply(ParquetTable table, string key)
        {
            var alignment = Align(table);
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
                        Upsert(Project(alignment, row), CompositeKey(table, row, key));
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

            return new AppliedDelta(upserted, ignoredDeletes, rejected, alignment.Drift);
        }

        public async Task WriteAsync(Stream output, CancellationToken cancellationToken)
        {
            if (_fields.Count == 0)
            {
                throw new InvalidOperationException($"Nothing to write for dataset '{definition.Name}': no file supplied a schema");
            }

            var fields = _fields.ToArray();

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

        /// <summary>Reconciles the file's columns with the output's, widening the output for any column
        /// it introduces, and returns where each output column is found in the file - or -1 when the
        /// file does not carry it.</summary>
        private Alignment Align(ParquetTable table)
        {
            // The first file establishes the schema rather than drifting from it, so its columns are
            // not reported as new.
            var establishing = _fields.Count == 0;
            var added = new List<string>();

            foreach (var field in table.Fields)
            {
                if (IsChangeType(field.Name) || _columnByName.ContainsKey(field.Name))
                {
                    continue;
                }

                _columnByName[field.Name] = _fields.Count;
                _fields.Add(field);
                added.Add(field.Name);
            }

            if (added.Count > 0)
            {
                Widen();
            }

            var indexes = new int[_fields.Count];
            var missing = new List<string>();

            for (var column = 0; column < _fields.Count; column++)
            {
                var name = _fields[column].Name;
                indexes[column] = table.IndexOf(name);

                if (indexes[column] < 0)
                {
                    missing.Add(name);
                }
            }

            return new Alignment(indexes, new SchemaDrift(missing, establishing ? [] : added));
        }

        /// <summary>Grows the rows already held so they carry a null for each newly added column.</summary>
        private void Widen()
        {
            for (var index = 0; index < _rows.Count; index++)
            {
                var row = _rows[index];
                Array.Resize(ref row, _fields.Count);
                _rows[index] = row;
            }
        }

        /// <summary>The row reduced to the output columns, in output column order, with a null for any
        /// column the file does not carry.</summary>
        private static string?[] Project(Alignment alignment, string?[] row)
        {
            var indexes = alignment.Indexes;
            var projected = new string?[indexes.Length];

            for (var column = 0; column < indexes.Length; column++)
            {
                projected[column] = indexes[column] < 0 ? null : row[indexes[column]];
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

        private sealed record Alignment(int[] Indexes, SchemaDrift Drift);
    }

    /// <summary>What one delta did to the merged state.</summary>
    private sealed record AppliedDelta(long Upserted, long IgnoredDeletes, long Rejected, SchemaDrift Drift);

    /// <summary>How one file's columns differed from the output's: <paramref name="Missing"/> columns the
    /// output carries and the file does not, <paramref name="Added"/> columns the file introduced.</summary>
    private sealed record SchemaDrift(IReadOnlyList<string> Missing, IReadOnlyList<string> Added)
    {
        public bool Any => Missing.Count > 0 || Added.Count > 0;
    }
}
