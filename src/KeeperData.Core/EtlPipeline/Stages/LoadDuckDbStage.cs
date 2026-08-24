using System.Runtime.CompilerServices;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Staging;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>Loads every dataset snapshot as a table into the single DuckDB staging database.
/// All snapshots -> one database. Materialises: staging/.
///
/// The database is named with the newest source timestamp of the snapshots it holds, so the whole
/// chain - normalised, snapshot, staging - carries one timestamp end to end, and a re-run over the
/// same snapshots resolves to the same key and does nothing.
///
/// It is built in a temporary directory and uploaded only once every table has been written and its
/// row count verified against its Parquet source, so a failure part way through leaves nothing in
/// staging/ rather than a partial database.</summary>
public sealed class LoadDuckDbStage(
    IEtlPipelineStorageProvider storageProvider,
    IStagingDatabaseWriter databaseWriter,
    ILogger<LoadDuckDbStage> logger) : IStage<SnapshotFile, StagingDatabase>
{
    public string Name => "load-duckdb";

    public async IAsyncEnumerable<StagingDatabase> RunAsync(
        IAsyncEnumerable<SnapshotFile> input,
        IPipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (context is EtlPipelineContext { Dataset: not null } filtered)
        {
            logger.LogInformation(
                "Dataset-filtered run for {Dataset} stops after snapshots; no partial shared staging database is produced",
                filtered.Dataset);
            yield break;
        }

        var snapshots = new List<SnapshotFile>();
        await foreach (var snapshot in input.WithCancellation(cancellationToken))
            snapshots.Add(snapshot);

        if (snapshots.Count == 0)
        {
            logger.LogInformation("No snapshots to load; no staging database produced");
            yield break;
        }

        yield return await LoadAsync(snapshots, cancellationToken);
    }

    private async Task<StagingDatabase> LoadAsync(
        IReadOnlyList<SnapshotFile> snapshots,
        CancellationToken cancellationToken)
    {
        var snapshotStorage = storageProvider.ForFolder(EtlPipelineFolders.Snapshots);
        var stagingStorage = storageProvider.ForFolder(EtlPipelineFolders.Staging);

        var sourceTimestamp = snapshots.Max(snapshot => snapshot.SourceTimestamp);
        var outputKey = StagingFileNaming.DatabaseKey(sourceTimestamp);
        var runId = snapshots[0].RunId;

        if (await stagingStorage.ExistsAsync(outputKey, cancellationToken))
        {
            logger.LogInformation("Staging database {DatabaseKey} already exists; reusing it", outputKey);

            return new StagingDatabase
            {
                RunId = runId,
                Key = outputKey,
                SourceTimestamp = sourceTimestamp,
                Created = false
            };
        }

        var workingDirectory = Directory.CreateTempSubdirectory("etl-staging-").FullName;

        try
        {
            var sources = await DownloadAsync(snapshots, snapshotStorage, workingDirectory, cancellationToken);

            var databasePath = Path.Combine(workingDirectory, "staging.duckdb");
            var result = await databaseWriter.WriteAsync(sources, databasePath, cancellationToken);

            await using (var published = await stagingStorage.OpenWriteAsync(
                outputKey, StagingFileNaming.DatabaseContentType, cancellationToken: cancellationToken))
            {
                await using var database = new FileStream(databasePath, FileMode.Open, FileAccess.Read);
                await database.CopyToAsync(published, cancellationToken);
            }

            logger.LogInformation(
                "Wrote staging database {DatabaseKey} with {TableCount} table(s): {RowCount} rows",
                outputKey, result.Tables.Count, result.Tables.Sum(table => table.RowCount));

            return new StagingDatabase
            {
                RunId = runId,
                Key = outputKey,
                SourceTimestamp = sourceTimestamp,
                Tables = result.Tables,
                Created = true
            };
        }
        finally
        {
            Directory.Delete(workingDirectory, recursive: true);
        }
    }

    private static async Task<IReadOnlyList<StagingTableSource>> DownloadAsync(
        IReadOnlyList<SnapshotFile> snapshots,
        IBlobStorageService snapshotStorage,
        string workingDirectory,
        CancellationToken cancellationToken)
    {
        var sources = new List<StagingTableSource>(snapshots.Count);

        foreach (var snapshot in snapshots)
        {
            var tableName = snapshot.Definition.Name;
            var parquetPath = Path.Combine(workingDirectory, $"{tableName}.parquet");

            await using (var reader = await snapshotStorage.OpenReadAsync(snapshot.Key, cancellationToken))
            {
                await using var local = File.Create(parquetPath);
                await reader.CopyToAsync(local, cancellationToken);
            }

            sources.Add(new StagingTableSource(tableName, parquetPath, snapshot.Key));
        }

        return sources;
    }
}
