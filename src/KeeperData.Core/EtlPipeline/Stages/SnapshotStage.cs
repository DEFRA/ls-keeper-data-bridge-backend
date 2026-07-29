using System.Runtime.CompilerServices;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>Produces one canonical Parquet file per dataset in snapshots/, named with the dataset's
/// clean name and a fresh ETL timestamp. Materialises: snapshots/.
///
/// Snapshot mode only: the latest normalised file for the dataset (by the timestamp in its name) is
/// copied as-is. Delta walking, primary-key matching and CHANGE_TYPE handling are deferred to LKPR-34.
///
/// Idempotent: a snapshot records the normalised key it came from, so a re-run with no new normalised
/// file reuses the existing snapshot instead of writing a duplicate. Existing snapshots are never
/// overwritten and never deleted.</summary>
public sealed class SnapshotStage(
    IEtlPipelineStorageProvider storageProvider,
    TimeProvider timeProvider,
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
            var snapshot = await SnapshotAsync(fileSet, normalised, snapshots, cancellationToken);

            if (snapshot is not null)
            {
                yield return snapshot;
            }
        }
    }

    private async Task<SnapshotFile?> SnapshotAsync(
        NormalisedFileSet fileSet,
        IBlobStorageService normalised,
        IBlobStorageService snapshots,
        CancellationToken cancellationToken)
    {
        var definition = fileSet.Definition;

        var sourceKey = SnapshotFileNaming.LatestByTimestamp(definition, await NormalisedKeysAsync(fileSet, normalised, cancellationToken));

        if (sourceKey is null)
        {
            logger.LogInformation("No normalised file for dataset {DataSet}; no snapshot produced", definition.Name);
            return null;
        }

        var existing = await FindSnapshotForAsync(definition, sourceKey, snapshots, cancellationToken);

        if (existing is not null)
        {
            logger.LogInformation(
                "Snapshot {SnapshotKey} already covers normalised file {SourceKey} for dataset {DataSet}; reusing it",
                existing, sourceKey, definition.Name);

            if (!SnapshotFileNaming.TryExtractTimestamp(definition, existing, out var existingTimestamp))
            {
                logger.LogWarning(
                    "Could not parse timestamp from existing snapshot key {SnapshotKey} for dataset {DataSet}; Timestamp will be default",
                    existing, definition.Name);
            }

            return new SnapshotFile(definition)
            {
                RunId = fileSet.RunId,
                Key = existing,
                SourceKey = sourceKey,
                Timestamp = existingTimestamp,
                Created = false
            };
        }

        var timestamp = timeProvider.GetUtcNow();
        var snapshotKey = SnapshotFileNaming.SnapshotKey(definition, timestamp);

        if (await snapshots.ExistsAsync(snapshotKey, cancellationToken))
        {
            logger.LogWarning(
                "Snapshot {SnapshotKey} for dataset {DataSet} already exists and will not be overwritten",
                snapshotKey, definition.Name);

            return null;
        }

        await CopyAsync(normalised, sourceKey, snapshots, snapshotKey, cancellationToken);

        logger.LogInformation(
            "Wrote snapshot {SnapshotKey} for dataset {DataSet} from normalised file {SourceKey}",
            snapshotKey, definition.Name, sourceKey);

        return new SnapshotFile(definition)
        {
            RunId = fileSet.RunId,
            Key = snapshotKey,
            SourceKey = sourceKey,
            Timestamp = timestamp,
            Created = true
        };
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

    /// <summary>The newest existing snapshot for the dataset, when it was produced from
    /// <paramref name="sourceKey"/>. Null when there is no snapshot yet or it is out of date.</summary>
    private static async Task<string?> FindSnapshotForAsync(
        DataSetDefinition definition,
        string sourceKey,
        IBlobStorageService snapshots,
        CancellationToken cancellationToken)
    {
        var existing = await snapshots.ListAsync(SnapshotFileNaming.DataSetPrefix(definition), cancellationToken);
        var latest = SnapshotFileNaming.LatestByTimestamp(definition, existing.Select(o => o.Key));

        if (latest is null)
        {
            return null;
        }

        var metadata = await snapshots.GetMetadataAsync(latest, cancellationToken);

        return metadata.UserMetadata.TryGetValue(EtlConstants.MetadataKeySnapshotSourceKey, out var recorded)
            && string.Equals(recorded, sourceKey, StringComparison.Ordinal)
                ? latest
                : null;
    }

    private static async Task CopyAsync(
        IBlobStorageService normalised,
        string sourceKey,
        IBlobStorageService snapshots,
        string snapshotKey,
        CancellationToken cancellationToken)
    {
        var metadata = new Dictionary<string, string> { { EtlConstants.MetadataKeySnapshotSourceKey, sourceKey } };

        await using var source = await normalised.OpenReadAsync(sourceKey, cancellationToken);
        await using var destination = await snapshots.OpenWriteAsync(
            snapshotKey, SnapshotFileNaming.ParquetContentType, metadata, cancellationToken: cancellationToken);

        await source.CopyToAsync(destination, cancellationToken);
    }
}
