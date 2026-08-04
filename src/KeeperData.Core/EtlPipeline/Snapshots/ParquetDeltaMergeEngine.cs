using KeeperData.Core.ETL.Impl;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.EtlPipeline.Snapshots;

/// <summary>Folds deltas onto a base snapshot in memory and writes the result as Parquet.
///
/// The state is keyed by the dataset's primary keys, so a row arriving again replaces the one already
/// held (last writer wins) and an unseen key is appended. The change type column describes the delta,
/// not the resulting state, so it is dropped from the output.</summary>
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

        if (baseSnapshot is not null)
        {
            var table = await ReadTableAsync(baseSnapshot, cancellationToken);
            state.SeedFrom(table, baseSnapshot.Key);
        }

        var upserted = 0L;
        var ignoredDeletes = 0L;
        var rejected = 0L;

        foreach (var delta in deltas)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var (Upserted, IgnoredDeletes, Rejected) = await ApplyDeltaAsync(state, delta, definition, cancellationToken);

            upserted += Upserted;
            ignoredDeletes += IgnoredDeletes;
            rejected += Rejected;
        }

        await state.WriteAsync(output, cancellationToken);

        return new DeltaMergeResult
        {
            DeltasApplied = deltas.Count,
            RowsUpserted = upserted,
            RowsIgnoredDeletes = ignoredDeletes,
            RowsRejected = rejected,
            RowCount = state.RowCount
        };
    }

    private async Task<(long Upserted, long IgnoredDeletes, long Rejected)> ApplyDeltaAsync(
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

    private static async Task<ParquetTable> ReadTableAsync(DeltaMergeSource source, CancellationToken cancellationToken)
    {
        await using var stream = await source.Open(cancellationToken);

        return await ParquetTable.ReadAsync(stream, source.Key, cancellationToken);
    }
}
