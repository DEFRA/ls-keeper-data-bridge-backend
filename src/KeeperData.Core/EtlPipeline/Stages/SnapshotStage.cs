using System.Runtime.CompilerServices;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Snapshots;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>Produces one Parquet snapshot per dataset in snapshots/, named with the latest source
/// timestamp it includes. Materialises: snapshots/.
///
/// Every source file is a delta - the first one is simply the largest - so a dataset in
/// <see cref="DataSetIngestionMode.Delta"/> folds each normalised file newer than the latest snapshot
/// onto it, oldest first. With no snapshot yet the fold starts from nothing and every normalised file
/// is applied. A dataset in <see cref="DataSetIngestionMode.Snapshot"/> keeps the simpler behaviour of
/// copying its latest normalised file as-is.
///
/// Ordering comes from the timestamp in the file name and nothing else. A file whose name carries no
/// timestamp, or two files carrying the same one, fail the import rather than being guessed at.
///
/// Resume state is the timestamp in the latest snapshot's own name: no metadata and no sidecar files.
/// A re-run with nothing newer reuses that snapshot. Snapshots are never overwritten or deleted.
///
/// A dataset with a baseline (bulk) lane additionally carries the hash of its bulk file names in the
/// snapshot name. A snapshot carrying the current hash is resumed from as usual; when none does, the
/// bulk set has changed and the dataset is rebuilt from every bulk file, followed by only those deltas
/// newer than the newest bulk. The older deltas are not replayed: the merge is last-writer-wins, so
/// applying a pre-baseline cut after the baseline would revert rows the baseline already carries.
/// Bulk files are a set - parts of one extract can share a timestamp - and only deltas are a sequence.</summary>
public sealed class SnapshotStage(
    IEtlPipelineStorageProvider storageProvider,
    IDeltaMergeEngine mergeEngine,
    ILogger<SnapshotStage> logger) : IStage<NormalisedFileSet, SnapshotFile>
{
    public string Name => "snapshot";

    public async IAsyncEnumerable<SnapshotFile> RunAsync(
        IAsyncEnumerable<NormalisedFileSet> input,
        IPipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var normalised = storageProvider.ForFolder(EtlPipelineFolders.Normalised);
        var snapshots = storageProvider.ForFolder(EtlPipelineFolders.Snapshots);

        await foreach (var fileSet in input.WithCancellation(cancellationToken))
        {
            var outcome = await SnapshotAsync(fileSet, normalised, snapshots, cancellationToken);

            if (outcome is SnapshotOutcome.Produced { File: var snapshot })
            {
                yield return snapshot;
            }
        }
    }

    private async Task<SnapshotOutcome> SnapshotAsync(
        NormalisedFileSet fileSet,
        IBlobStorageService normalised,
        IBlobStorageService snapshots,
        CancellationToken cancellationToken)
    {
        var definition = fileSet.Definition;

        var keys = await NormalisedKeysAsync(fileSet, normalised, cancellationToken);

        var baseline = Baseline(definition, keys);
        var baselineHash = baseline.Count == 0 ? null : BaselineHash.Compute(baseline.Select(file => file.Key));

        var deltas = SnapshotFileNaming.OrderedByTimestamp(
            definition, keys.Where(key => !DataSetFileNaming.MatchesBaseline(definition, key)));

        if (baseline.Count == 0 && deltas.Count == 0)
        {
            logger.LogInformation("No normalised file for dataset {DataSet}; no snapshot produced", definition.Name);
            return SnapshotOutcome.None;
        }

        var current = await LatestSnapshotAsync(definition, snapshots, baselineHash, cancellationToken);

        var plan = current is null
            ? Reset(baseline, deltas)
            : Resume(current, deltas);

        if (plan is null)
        {
            logger.LogInformation(
                "Dataset {DataSet} has nothing newer than snapshot {SnapshotKey}; reusing it",
                definition.Name, current!.Key);

            return SnapshotOutcome.Of(Reused(fileSet, current));
        }

        var outputKey = SnapshotFileNaming.SnapshotKey(definition, plan.Timestamp, baselineHash);

        if (await snapshots.ExistsAsync(outputKey, cancellationToken))
        {
            logger.LogWarning(
                "Snapshot {SnapshotKey} for dataset {DataSet} already exists and will not be overwritten",
                outputKey, definition.Name);

            return SnapshotOutcome.Of(Reused(fileSet, new TimestampedKey(outputKey, plan.Timestamp)));
        }

        var file = definition.IngestionMode == DataSetIngestionMode.Delta
            ? await MergeAsync(fileSet, current, plan, outputKey, normalised, snapshots, cancellationToken)
            : await CopyAsync(fileSet, plan.Applied[^1], outputKey, normalised, snapshots, cancellationToken);

        return SnapshotOutcome.Of(file);
    }

    /// <summary>Fold onto the snapshot built from the same bulk set, or nothing left to do.</summary>
    private static SnapshotPlan? Resume(TimestampedKey current, IReadOnlyList<TimestampedKey> deltas)
    {
        IReadOnlyList<TimestampedKey> pending = [.. deltas.Where(delta => delta.Timestamp > current.Timestamp)];

        return pending.Count == 0 ? null : new SnapshotPlan(pending, pending[^1].Timestamp);
    }

    /// <summary>Rebuild from the bulk files, then the deltas cut after them. A baseline with no deltas
    /// yet is named after the newest bulk, and a delta with no rows still moves the name on, or the
    /// header-only files in the feed would be reprocessed on every run forever.</summary>
    private static SnapshotPlan? Reset(
        IReadOnlyList<TimestampedKey> baseline,
        IReadOnlyList<TimestampedKey> deltas)
    {
        if (baseline.Count == 0)
        {
            return deltas.Count == 0 ? null : new SnapshotPlan(deltas, deltas[^1].Timestamp);
        }

        var newestBaseline = baseline.Max(file => file.Timestamp);
        IReadOnlyList<TimestampedKey> pending = [.. deltas.Where(delta => delta.Timestamp > newestBaseline)];

        return new SnapshotPlan(
            [.. baseline, .. pending],
            pending.Count == 0 ? newestBaseline : pending[^1].Timestamp);
    }

    /// <summary>The dataset's bulk files, ordered by key. They are a set rather than a sequence: parts
    /// of one extract can share a timestamp, so they must never meet the duplicate check in
    /// <see cref="SnapshotFileNaming.OrderedByTimestamp"/>, and the order they are applied in cannot
    /// matter because they are disjoint cuts of the same baseline.</summary>
    private static IReadOnlyList<TimestampedKey> Baseline(DataSetDefinition definition, IEnumerable<string> keys)
        => [.. keys
            .Where(key => DataSetFileNaming.MatchesBaseline(definition, key))
            .OrderBy(key => key, StringComparer.Ordinal)
            .Select(key => new TimestampedKey(key, DataSetFileNaming.ExtractTimestamp(definition, key)))];

    /// <summary>The files this run applies, oldest first, and the timestamp naming the result.</summary>
    private sealed record SnapshotPlan(IReadOnlyList<TimestampedKey> Applied, DateTimeOffset Timestamp);

    /// <summary>Folds the pending deltas onto the current snapshot. The merge runs into a local
    /// temporary file and is uploaded only once it has completed, so a failure part way through never
    /// leaves a half-written object under the snapshot's key.</summary>
    private async Task<SnapshotFile> MergeAsync(
        NormalisedFileSet fileSet,
        TimestampedKey? current,
        SnapshotPlan plan,
        string outputKey,
        IBlobStorageService normalised,
        IBlobStorageService snapshots,
        CancellationToken cancellationToken)
    {
        var definition = fileSet.Definition;
        var pending = plan.Applied;
        var workingFile = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        try
        {
            DeltaMergeResult result;

            await using (var working = new FileStream(workingFile, FileMode.Create, FileAccess.ReadWrite))
            {
                result = await mergeEngine.MergeAsync(
                    definition,
                    current is null ? null : Source(snapshots, current.Key),
                    [.. pending.Select(file => Source(normalised, file.Key))],
                    working,
                    cancellationToken);
            }

            await using (var published = await snapshots.OpenWriteAsync(
                outputKey, SnapshotFileNaming.ParquetContentType, cancellationToken: cancellationToken))
            {
                await using var merged = new FileStream(workingFile, FileMode.Open, FileAccess.Read);
                await merged.CopyToAsync(published, cancellationToken);
            }

            logger.LogInformation(
                "Wrote snapshot {SnapshotKey} for dataset {DataSet} from {DeltaCount} delta(s) onto {BaseKey}: {RowCount} rows",
                outputKey, definition.Name, pending.Count, current?.Key ?? "no previous snapshot", result.RowCount);

            return new SnapshotFile(definition)
            {
                RunId = fileSet.RunId,
                Key = outputKey,
                SourceTimestamp = plan.Timestamp,
                AppliedKeys = [.. pending.Select(file => file.Key)],
                Created = true,
                RowCount = result.RowCount,
                RowsUpserted = result.RowsUpserted,
                RowsDeleted = result.RowsDeleted,
                RowsIgnoredDeletes = result.RowsIgnoredDeletes,
                RowsRejected = result.RowsRejected,
                ColumnsNullified = result.ColumnsNullified,
                ColumnsAdded = result.ColumnsAdded
            };
        }
        finally
        {
            File.Delete(workingFile);
        }
    }

    /// <summary>Snapshot mode: the latest normalised file becomes the snapshot unchanged.</summary>
    private async Task<SnapshotFile> CopyAsync(
        NormalisedFileSet fileSet,
        TimestampedKey source,
        string outputKey,
        IBlobStorageService normalised,
        IBlobStorageService snapshots,
        CancellationToken cancellationToken)
    {
        await using (var reader = await normalised.OpenReadAsync(source.Key, cancellationToken))
        {
            await using var writer = await snapshots.OpenWriteAsync(
                outputKey, SnapshotFileNaming.ParquetContentType, cancellationToken: cancellationToken);

            await reader.CopyToAsync(writer, cancellationToken);
        }

        logger.LogInformation(
            "Wrote snapshot {SnapshotKey} for dataset {DataSet} from normalised file {SourceKey}",
            outputKey, fileSet.Definition.Name, source.Key);

        return new SnapshotFile(fileSet.Definition)
        {
            RunId = fileSet.RunId,
            Key = outputKey,
            SourceTimestamp = source.Timestamp,
            AppliedKeys = [source.Key],
            Created = true
        };
    }

    private static SnapshotFile Reused(NormalisedFileSet fileSet, TimestampedKey snapshot)
        => new(fileSet.Definition)
        {
            RunId = fileSet.RunId,
            Key = snapshot.Key,
            SourceTimestamp = snapshot.Timestamp,
            Created = false
        };

    private static DeltaMergeSource Source(IBlobStorageService storage, string key)
        => new(key, token => storage.OpenReadAsync(key, token));

    /// <summary>The dataset's newest snapshot, or null when it has none yet.</summary>
    private static async Task<TimestampedKey?> LatestSnapshotAsync(
        DataSetDefinition definition,
        IBlobStorageService snapshots,
        string? baselineHash,
        CancellationToken cancellationToken)
    {
        var existing = await snapshots.ListAsync(SnapshotFileNaming.DataSetPrefix(definition), cancellationToken);

        var latest = SnapshotFileNaming.LatestForHash(definition, existing.Select(o => o.Key), baselineHash);

        return latest is null || !SnapshotFileNaming.TryExtractTimestamp(definition, latest, out var timestamp)
            ? null
            : new TimestampedKey(latest, timestamp);
    }

    private abstract record SnapshotOutcome
    {
        public static readonly SnapshotOutcome None = new Skipped();
        public static SnapshotOutcome Of(SnapshotFile file) => new Produced(file);

        public sealed record Produced(SnapshotFile File) : SnapshotOutcome;
        private sealed record Skipped : SnapshotOutcome;
    }

    /// <summary>The normalised keys the payload carries, falling back to listing the dataset's folder
    /// while the normalise stage does not yet populate them.</summary>
    private static async Task<IReadOnlyList<string>> NormalisedKeysAsync(
        NormalisedFileSet fileSet,
        IBlobStorageService normalised,
        CancellationToken cancellationToken)
    {
        if (fileSet.Files.Count > 0)
        {
            return fileSet.Files;
        }

        var objects = await normalised.ListAsync(SnapshotFileNaming.DataSetPrefix(fileSet.Definition), cancellationToken);

        return [.. objects.Select(o => o.Key)];
    }
}
