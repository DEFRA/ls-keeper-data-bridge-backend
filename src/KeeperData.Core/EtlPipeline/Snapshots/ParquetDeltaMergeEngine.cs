using KeeperData.Core.ETL.Impl;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.EtlPipeline.Snapshots;

/// <summary>Folds deltas onto a base snapshot in memory and writes the result as Parquet.
///
/// The state is keyed by the dataset's primary keys, so a row arriving again replaces the one already
/// held (last writer wins) and an unseen key is appended. The change type column describes the delta,
/// not the resulting state, so it is dropped from the output.
///
/// A source extract can gain or lose a column between files, so the output schema is the union of what
/// every file supplies rather than whatever the first file happened to carry. Either kind of drift is
/// tolerated and warned about; only a missing primary key column still fails the merge.</summary>
public sealed partial class ParquetDeltaMergeEngine(ILogger<ParquetDeltaMergeEngine> logger) : IDeltaMergeEngine
{
    public async Task<DeltaMergeResult> MergeAsync(
        DataSetDefinition definition,
        DeltaMergeSource? baseSnapshot,
        IReadOnlyList<DeltaMergeSource> deltas,
        Stream output,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(deltas);
        ArgumentNullException.ThrowIfNull(output);

        var state = new MergeState(definition);
        var nullified = new OrderedSet();
        var added = new OrderedSet();

        if (baseSnapshot is not null)
        {
            var table = await ReadTableAsync(baseSnapshot, cancellationToken);
            var drift = state.SeedFrom(table, baseSnapshot.Key);

            Report(drift, baseSnapshot.Key, definition, nullified, added);
        }

        var upserted = 0L;
        var ignoredDeletes = 0L;
        var rejected = 0L;

        foreach (var delta in deltas)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var applied = await ApplyDeltaAsync(state, delta, definition, cancellationToken);

            upserted += applied.Upserted;
            ignoredDeletes += applied.IgnoredDeletes;
            rejected += applied.Rejected;

            Report(applied.Drift, delta.Key, definition, nullified, added);
        }

        await state.WriteAsync(output, cancellationToken);

        return new DeltaMergeResult
        {
            DeltasApplied = deltas.Count,
            RowsUpserted = upserted,
            RowsIgnoredDeletes = ignoredDeletes,
            RowsRejected = rejected,
            RowCount = state.RowCount,
            ColumnsNullified = nullified.Values,
            ColumnsAdded = added.Values
        };
    }

    private async Task<AppliedDelta> ApplyDeltaAsync(
        MergeState state,
        DeltaMergeSource delta,
        DataSetDefinition definition,
        CancellationToken cancellationToken)
    {
        var table = await ReadTableAsync(delta, cancellationToken);
        var applied = state.Apply(table, delta.Key);

        if (applied.Rejected > 0)
        {
            logger.LogWarning(
                "Ignored {RejectedRows} row(s) with an unrecognised {ChangeTypeColumn} in {DeltaKey} for dataset {DataSet}",
                applied.Rejected, definition.ChangeTypeHeaderName, delta.Key, definition.Name);
        }

        logger.LogInformation(
            "Applied delta {DeltaKey} to dataset {DataSet}: {Upserted} upserted, {IgnoredDeletes} delete row(s) ignored",
            delta.Key, definition.Name, applied.Upserted, applied.IgnoredDeletes);

        return applied;
    }

    /// <summary>A column appearing or disappearing is allowed but never silent: the snapshot's shape has
    /// changed, and whoever reads it downstream has had no say in it. It goes to the log with the file
    /// that caused it, and onto the run's status so it is visible without reading logs.</summary>
    private void Report(
        SchemaDrift drift,
        string key,
        DataSetDefinition definition,
        OrderedSet nullified,
        OrderedSet added)
    {
        nullified.AddRange(drift.Missing);
        added.AddRange(drift.Added);

        if (drift.Missing.Count > 0)
        {
            logger.LogWarning(
                "{FileKey} does not carry column(s) {MissingColumns} held by dataset {DataSet}; they are null for the rows it supplies",
                key, string.Join(", ", drift.Missing), definition.Name);
        }

        if (drift.Added.Count > 0)
        {
            logger.LogWarning(
                "{FileKey} introduces column(s) {AddedColumns} to dataset {DataSet}; they are null for the rows already held",
                key, string.Join(", ", drift.Added), definition.Name);
        }
    }

    private static async Task<ParquetTable> ReadTableAsync(DeltaMergeSource source, CancellationToken cancellationToken)
    {
        await using var stream = await source.Open(cancellationToken);

        return await ParquetTable.ReadAsync(stream, source.Key, cancellationToken);
    }

    /// <summary>The same column can drift in more than one file; status reports it once, in the order
    /// it was first seen.</summary>
    private sealed class OrderedSet
    {
        private readonly List<string> _values = [];
        private readonly HashSet<string> _seen = new(StringComparer.OrdinalIgnoreCase);

        public IReadOnlyList<string> Values => _values;

        public void AddRange(IReadOnlyList<string> names)
        {
            foreach (var name in names)
            {
                if (_seen.Add(name))
                {
                    _values.Add(name);
                }
            }
        }
    }
}
