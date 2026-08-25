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
/// A re-run with nothing newer reuses that snapshot. Snapshots are never overwritten or deleted.</summary>
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

        var available = SnapshotFileNaming.OrderedByTimestamp(
            definition, await NormalisedKeysAsync(fileSet, normalised, cancellationToken));

        if (available.Count == 0)
        {
            logger.LogInformation("No normalised file for dataset {DataSet}; no snapshot produced", definition.Name);
            return SnapshotOutcome.None;
        }

        var current = await LatestSnapshotAsync(definition, snapshots, cancellationToken);

        var pending = current is null
            ? available
            : [.. available.Where(file => file.Timestamp > current.Timestamp)];

        if (pending.Count == 0)
        {
            logger.LogInformation(
                "Dataset {DataSet} has nothing newer than snapshot {SnapshotKey}; reusing it",
                definition.Name, current!.Key);

            return SnapshotOutcome.Of(Reused(fileSet, current));
        }

        var outputKey = SnapshotFileNaming.SnapshotKey(definition, pending[^1].Timestamp);

        if (await snapshots.ExistsAsync(outputKey, cancellationToken))
        {
            logger.LogWarning(
                "Snapshot {SnapshotKey} for dataset {DataSet} already exists and will not be overwritten",
                outputKey, definition.Name);

            return SnapshotOutcome.Of(Reused(fileSet, new TimestampedKey(outputKey, pending[^1].Timestamp)));
        }

        var file = definition.IngestionMode == DataSetIngestionMode.Delta
            ? await MergeAsync(fileSet, current, pending, outputKey, normalised, snapshots, cancellationToken)
            : await CopyAsync(fileSet, pending[^1], outputKey, normalised, snapshots, cancellationToken);

        return SnapshotOutcome.Of(file);
    }

    /// <summary>Folds the pending deltas onto the current snapshot. The merge runs into a local
    /// temporary file and is uploaded only once it has completed, so a failure part way through never
    /// leaves a half-written object under the snapshot's key.</summary>
    private async Task<SnapshotFile> MergeAsync(
        NormalisedFileSet fileSet,
        TimestampedKey? current,
        IReadOnlyList<TimestampedKey> pending,
        string outputKey,
        IBlobStorageService normalised,
        IBlobStorageService snapshots,
        CancellationToken cancellationToken)
    {
        var definition = fileSet.Definition;
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
                SourceTimestamp = pending[^1].Timestamp,
                AppliedKeys = [.. pending.Select(file => file.Key)],
                Created = true,
                RowCount = result.RowCount,
                RowsUpserted = result.RowsUpserted,
                RowsIgnoredDeletes = result.RowsIgnoredDeletes,
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
        CancellationToken cancellationToken)
    {
        var existing = await snapshots.ListAsync(SnapshotFileNaming.DataSetPrefix(definition), cancellationToken);

        return SnapshotFileNaming.OrderedByTimestamp(definition, existing.Select(o => o.Key)).LastOrDefault();
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
