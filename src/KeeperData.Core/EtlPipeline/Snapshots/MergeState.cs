using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Optimise;
using KeeperData.Core.EtlPipeline.Parquet;
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
    /// Primary keys are not treated this way - a file that cannot be keyed still fails the merge.
    ///
    /// A deleted row is tombstoned rather than removed. The key-to-row map holds indices, so compacting
    /// the list would shift every later row out from under its key; the slot is emptied instead and
    /// skipped on write, which also lets a later insert for the same key reinstate it.</summary>
    private sealed class MergeState(DataSetDefinition definition)
    {
        private readonly Dictionary<string, int> _indexByKey = new(StringComparer.Ordinal);

        /// <summary>Null marks a tombstone: a row deleted by a delta, whose slot is held so the indices
        /// of the rows after it stay valid.</summary>
        private readonly List<string?[]?> _rows = [];

        private readonly List<DataField> _fields = [];
        private readonly Dictionary<string, int> _columnByName = new(StringComparer.OrdinalIgnoreCase);

        public long RowCount => _rows.Count(row => row is not null);

        public SchemaDrift SeedFrom(ParquetTable table, string key)
        {
            var alignment = Align(table);

            foreach (var row in table.Rows)
            {
                Upsert(Project(alignment, row), CompositeKey(table, row, key));
            }

            return alignment.Drift;
        }

        /// <summary>
        /// Folds one delta's rows onto the state. Insert and update are both upserts: neither is trusted
        /// to be exclusively one or the other, because a replay after a rebuild presents the same insert
        /// twice. A change type that is neither, nor a delete, is rejected and counted rather than
        /// guessed at as an upsert - the row's intent is unknown, and writing it would be a guess about
        /// data.
        ///
        /// A delete is applied for a dataset whose deltas describe their own ordering, and counted but
        /// ignored for the datasets whose feeds have always been treated that way.
        /// </summary>
        public AppliedDelta Apply(ParquetTable table, string key)
        {
            var alignment = Align(table);
            var changeTypeIndex = table.IndexOf(definition.ChangeTypeHeaderName);

            var upserted = 0L;
            var deleted = 0L;
            var ignoredDeletes = 0L;
            var rejected = 0L;

            foreach (var row in InAuditOrder(table))
            {
                var changeType = changeTypeIndex < 0 ? ChangeType.Insert : row[changeTypeIndex];

                switch (changeType)
                {
                    case ChangeType.Insert:
                    case ChangeType.Update:
                        Upsert(Project(alignment, row), CompositeKey(table, row, key));
                        upserted++;
                        break;

                    case ChangeType.Delete when definition.Audit is null:
                        ignoredDeletes++;
                        break;

                    case ChangeType.Delete:
                        if (Tombstone(CompositeKey(table, row, key)))
                        {
                            deleted++;
                        }

                        break;

                    default:
                        rejected++;
                        break;
                }
            }

            return new AppliedDelta(upserted, deleted, ignoredDeletes, rejected, alignment.Drift);
        }

        /// <summary>
        /// The file's rows in the order they must be applied. A dataset whose deltas carry their own
        /// sequence is ordered by it, because the merge is last-writer-wins and one file can carry two
        /// cuts of the same row: the sample updates one location twice inside a single file. File order
        /// agrees there, but nothing in the feed guarantees it.
        ///
        /// The sequence is shared with the other tables in the same extract, so it has gaps. A gap says
        /// nothing about a missing file and is not treated as one - only the relative order is read. A
        /// row whose sequence is absent or unparsable cannot be placed in it, so it keeps its file order
        /// ahead of the rows that can, which leaves a sequenced cut of the same key winning over it.
        /// </summary>
        private IEnumerable<string?[]> InAuditOrder(ParquetTable table)
        {
            if (definition.Audit is null)
            {
                return table.Rows;
            }

            var sequenceIndex = table.IndexOf(definition.Audit.SequenceColumn);

            return sequenceIndex < 0
                ? table.Rows
                : table.Rows.OrderBy(row => Sequence(row[sequenceIndex]));
        }

        private static long? Sequence(string? value)
            => long.TryParse(value, out var sequence) ? sequence : null;

        public async Task WriteAsync(Stream output, CancellationToken cancellationToken)
        {
            if (_fields.Count == 0)
                throw new InvalidOperationException($"Nothing to write for dataset '{definition.Name}': no file supplied a schema");

            var fields = _fields.ToArray();
            var rows = _rows.Where(row => row is not null).ToList();

            // A column non-nullable where it arrived can still hold nulls in the merged output - a
            // file that dropped it leaves nulls behind - so it is widened before the schema is built.
            for (var column = 0; column < fields.Length; column++)
            {
                if (!fields[column].IsNullable && rows.Exists(row => row![column] is null))
                {
                    fields[column] = AsNullable(fields[column]);
                }
            }

            await using var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), output, cancellationToken: cancellationToken);
            using var rowGroup = writer.CreateRowGroup();

            for (var column = 0; column < fields.Length; column++)
            {
                var field = fields[column];
                var values = Array.CreateInstance(ParquetColumns.ElementType(field), rows.Count);

                for (var row = 0; row < rows.Count; row++)
                {
                    // Canonical text back to the field's type: the inverse of the read, so it cannot
                    // fail for a value the read produced.
                    values.SetValue(ParquetValueText.Parse(ParquetColumns.ElementType(field), rows[row]![column]), row);
                }

                await ParquetColumns.WriteAsync(rowGroup, field, values, cancellationToken);
            }
        }

        private static DataField AsNullable(DataField field)
            => field is DecimalDataField decimalField
                ? new DecimalDataField(field.Name, decimalField.Precision, decimalField.Scale, isNullable: true)
                : new DataField(field.Name, Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType, isNullable: true);

        /// <summary>Reconciles the file's columns with the output's, widening the output for any column
        /// it introduces, and returns where each output column is found in the file - or -1 when the
        /// file does not carry it.</summary>
        private Alignment Align(ParquetTable table)
        {
            var establishing = _fields.Count == 0;

            var added = MergeNewColumns(table);
            var (indexes, missing, retyped, declined) = BuildIndexMap(table);

            return new Alignment(indexes, new SchemaDrift(missing, establishing ? [] : added, retyped, declined));
        }

        /// <summary>Adds any column the file introduces to the output schema, widening the rows already
        /// held so they carry a null for it. Returns the names added, in the order they were found.</summary>
        private List<string> MergeNewColumns(ParquetTable table)
        {
            var added = new List<string>();

            foreach (var field in table.Fields)
            {
                if (IsSuppressed(field.Name) || _columnByName.ContainsKey(field.Name))
                    continue;

                _columnByName[field.Name] = _fields.Count;
                _fields.Add(field);
                added.Add(field.Name);
            }

            if (added.Count > 0) Widen();

            return added;
        }

        /// <summary>For each output column, where it is found in the file - or -1 when the file does not
        /// carry it - alongside the names of any output columns the file is missing, the columns whose
        /// type changed, and the columns whose upgrade was declined.
        ///
        /// A column two files disagree on the type of widens to string rather than failing the merge:
        /// the values are already held as canonical text, so a string column keeps every cut of the data
        /// legible. A column established as text goes the other way when a typed file arrives: if every
        /// held value survives conversion the column adopts the incoming type, so a snapshot that
        /// predates typed input migrates instead of pinning the column to string forever.</summary>
        private (int[] Indexes, List<string> Missing, List<RetypedColumn> Retyped, List<RetypedColumn> Declined) BuildIndexMap(ParquetTable table)
        {
            var indexes = new int[_fields.Count];
            var missing = new List<string>();
            var retyped = new List<RetypedColumn>();
            var declined = new List<RetypedColumn>();

            for (var column = 0; column < _fields.Count; column++)
            {
                var name = _fields[column].Name;
                indexes[column] = table.IndexOf(name);

                if (indexes[column] < 0)
                {
                    missing.Add(name);
                    continue;
                }

                var held = _fields[column];
                var incoming = table.Fields[indexes[column]];

                if (SameType(held, incoming))
                {
                    continue;
                }

                if (IsText(held))
                {
                    if (IsText(incoming))
                    {
                        continue;
                    }

                    if (CanRetype(column, incoming))
                    {
                        retyped.Add(new RetypedColumn(name, TypeName(held), TypeName(incoming)));
                        _fields[column] = incoming;
                    }
                    else
                    {
                        declined.Add(new RetypedColumn(name, TypeName(held), TypeName(incoming)));
                    }
                }
                else
                {
                    retyped.Add(new RetypedColumn(name, TypeName(held), TypeName(incoming)));
                    _fields[column] = new DataField<string?>(name);
                }
            }

            return (indexes, missing, retyped, declined);
        }

        /// <summary>A text column a file carries typed upgrades only when every value already held can
        /// take the type's canonical form. "007" parses as a number but loses its zeros, so it keeps
        /// the column text rather than rewriting the values, the same standard the optimise detector
        /// applies.
        ///
        /// Values whose meaning survives the rewrite get a second chance: a date in a feed's own
        /// datetime shape, or "true"/"FALSE" for a boolean, parses to the type even though it is not
        /// canonical text, so it is rewritten to canonical form as the type adopts rather than
        /// declining the column - the write path parses strictly, so the value has to be canonical
        /// before the field commits. A value matching no shape still declines.</summary>
        private bool CanRetype(int column, DataField incoming)
        {
            var type = ParquetColumns.ElementType(incoming);
            List<(int Row, string Canonical)>? rewrites = null;

            for (var index = 0; index < _rows.Count; index++)
            {
                var value = _rows[index]?[column];

                if (value is null || IsCanonical(type, value))
                {
                    continue;
                }

                if (!TryNormalize(type, value, out var canonical))
                {
                    return false;
                }

                (rewrites ??= []).Add((index, canonical!));
            }

            if (rewrites is not null)
            {
                foreach (var (row, canonical) in rewrites)
                {
                    _rows[row]![column] = canonical;
                }
            }

            return true;
        }

        /// <summary>The strict test every type must pass: the held value parses and its canonical
        /// form is the held text exactly, so adopting the type rewrites nothing.</summary>
        private static bool IsCanonical(Type type, string value)
            => ParquetValueText.TryParse(type, value, out var parsed)
                && string.Equals(ParquetValueText.Format(parsed), value, StringComparison.Ordinal);

        /// <summary>The source shapes a held value can take and still carry one agreed meaning, so
        /// it can be rewritten to canonical text: the forms the optimise detector accepts for date
        /// and time values, and true/false in any casing for booleans - the only two values the
        /// type has. Every other type declines: a value outside the canonical form has no agreed
        /// meaning to write.</summary>
        private static bool TryNormalize(Type type, string value, out string? canonical)
        {
            canonical = null;

            var target = Nullable.GetUnderlyingType(type) ?? type;
            object? parsed = null;

            if (target == typeof(DateTime) && ColumnTypeDetector.TryParseTimestamp(value, out var timestamp))
            {
                parsed = timestamp;
            }
            else if (target == typeof(DateOnly) && ColumnTypeDetector.TryParseDate(value, out var date))
            {
                parsed = date;
            }
            else if (target == typeof(bool) && bool.TryParse(value, out var flag))
            {
                parsed = flag;
            }

            if (parsed is null)
            {
                return false;
            }

            canonical = ParquetValueText.Format(parsed);
            return true;
        }

        private static bool SameType(DataField a, DataField b)
        {
            var typeA = Nullable.GetUnderlyingType(a.ClrType) ?? a.ClrType;
            var typeB = Nullable.GetUnderlyingType(b.ClrType) ?? b.ClrType;

            return typeA == typeB
                && (a is not DecimalDataField decimalA
                    || b is DecimalDataField decimalB && decimalA.Precision == decimalB.Precision && decimalA.Scale == decimalB.Scale);
        }

        /// <summary>Parquet.Net declares a string field's CLR type as ReadOnlyMemory&lt;char&gt; and a
        /// byte array's as ReadOnlyMemory&lt;byte&gt;; both are already the widest form a column can take.</summary>
        private static bool IsText(DataField field)
        {
            var type = Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType;

            return type == typeof(string) || type == typeof(ReadOnlyMemory<char>) || type == typeof(ReadOnlyMemory<byte>);
        }

        private static string TypeName(DataField field)
        {
            var type = Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType;

            return type == typeof(ReadOnlyMemory<char>) ? "string"
                : type == typeof(ReadOnlyMemory<byte>) ? "byte[]"
                : type.Name;
        }

        /// <summary>Grows the rows already held so they carry a null for each newly added column.</summary>
        private void Widen()
        {
            for (var index = 0; index < _rows.Count; index++)
            {
                var row = _rows[index];

                if (row is null)
                {
                    continue;
                }

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

        /// <summary>Empties the row's slot, keeping its key pointing at it so a later insert for the same
        /// key reinstates the row rather than appending a second one. False when the key is not held: a
        /// delete for a row the snapshot never carried has nothing to do.</summary>
        private bool Tombstone(string compositeKey)
        {
            if (!_indexByKey.TryGetValue(compositeKey, out var index) || _rows[index] is null)
            {
                return false;
            }

            _rows[index] = null;

            return true;
        }

        private string CompositeKey(ParquetTable table, string?[] row, string key)
        {
            var parts = definition.PrimaryKeyHeaderNames.Select(name =>
            {
                var index = table.IndexOf(name);

                return index < 0
                    ? throw new InvalidOperationException(
                        $"'{key}' has no primary key column '{name}' for dataset '{definition.Name}'. " +
                        $"It carries: {string.Join(", ", table.Fields.Select(field => field.Name))}")
                    : row[index] ?? string.Empty;
            });

            return string.Join(EtlConstants.CompositeKeyDelimiter, parts);
        }

        /// <summary>Columns the snapshot does not carry: the change type, which describes the delta
        /// rather than the resulting state, and the dataset's excluded columns - the audit columns and
        /// the per-file counters, which mean nothing once rows from many files are merged.</summary>
        private bool IsSuppressed(string name)
            => string.Equals(name, definition.ChangeTypeHeaderName, StringComparison.OrdinalIgnoreCase)
                || definition.ExcludedColumns.Contains(name, StringComparer.OrdinalIgnoreCase);

        private sealed record Alignment(int[] Indexes, SchemaDrift Drift);
    }

    /// <summary>What one delta did to the merged state.</summary>
    private sealed record AppliedDelta(long Upserted, long Deleted, long IgnoredDeletes, long Rejected, SchemaDrift Drift);

    /// <summary>How one file's columns differed from the output's: <paramref name="Missing"/> columns the
    /// output carries and the file does not, <paramref name="Added"/> columns the file introduced,
    /// <paramref name="Retyped"/> columns whose type changed - widened to string, or adopted the file's
    /// type - and <paramref name="Declined"/> columns that stayed text because a held value would not
    /// survive the conversion.</summary>
    private sealed record SchemaDrift(IReadOnlyList<string> Missing, IReadOnlyList<string> Added, IReadOnlyList<RetypedColumn> Retyped, IReadOnlyList<RetypedColumn> Declined);

    /// <summary>A column a later file carries as a different type than the output established.</summary>
    private sealed record RetypedColumn(string Name, string HeldType, string IncomingType);
}
