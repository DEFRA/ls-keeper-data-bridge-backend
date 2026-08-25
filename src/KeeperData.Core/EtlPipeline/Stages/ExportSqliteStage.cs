using System.Runtime.CompilerServices;
using System.Globalization;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.EtlPipeline.Views;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.EtlPipeline.Stages;

/// <summary>Transforms the DuckDB staging database into the SQLite read model. Materialises: views/.
///
/// The database is named with the staging database's source timestamp, so the whole chain -
/// normalised, snapshot, staging, views - carries one timestamp end to end.
///
/// It is built in a temporary directory and uploaded only once the transformation has completed, so
/// a failure anywhere in the script leaves nothing in views/ rather than a half-built read model.
/// Ephemeral task storage is assumed throughout: nothing survives the run.</summary>
public sealed class ExportSqliteStage(
    IEtlPipelineStorageProvider storageProvider,
    ISqliteViewWriter viewWriter,
    ILogger<ExportSqliteStage> logger) : IStage<StagingDatabase, SqliteExportFile>
{
    public string Name => "export-sqlite";

    public async IAsyncEnumerable<SqliteExportFile> RunAsync(
        IAsyncEnumerable<StagingDatabase> input,
        IPipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var database in input.WithCancellation(cancellationToken))
        {
            // Not Tables.Count: a reused staging database reports no tables but is still there to
            // export from.
            if (string.IsNullOrEmpty(database.Key))
            {
                logger.LogInformation("No staging database to export; no SQLite view produced");
                continue;
            }

            yield return await ExportAsync(database, cancellationToken);
        }
    }

    private async Task<SqliteExportFile> ExportAsync(StagingDatabase database, CancellationToken cancellationToken)
    {
        var stagingStorage = storageProvider.ForFolder(EtlPipelineFolders.Staging);
        var viewsStorage = storageProvider.ForFolder(EtlPipelineFolders.Views);

        var outputKey = ViewsFileNaming.DatabaseKey(database.SourceTimestamp);

        var existingTables = await GetExistingTablesAsync(viewsStorage, outputKey, cancellationToken);

        if (existingTables is not null)
        {
            logger.LogInformation(
                "SQLite view {ViewKey} already exists for transformation {Version}; reusing it",
                outputKey, SqliteViewDefinition.Version);

            return new SqliteExportFile
            {
                RunId = database.RunId,
                Key = outputKey,
                SourceTimestamp = database.SourceTimestamp,
                Tables = existingTables,
                Created = false
            };
        }

        var workingDirectory = Directory.CreateTempSubdirectory("etl-views-").FullName;

        try
        {
            var sourcePath = Path.Combine(workingDirectory, "staging.duckdb");
            await DownloadAsync(stagingStorage, database.Key, sourcePath, cancellationToken);

            var targetPath = Path.Combine(workingDirectory, "krds-db.sqlite");

            var result = await viewWriter.WriteAsync(
                new SqliteViewWriteRequest(
                    sourcePath, targetPath, SqliteViewDefinition.Sql, SqliteViewDefinition.TableNames),
                cancellationToken);

            await EtlArtefactWrite.RunAsync(
                viewsStorage,
                outputKey,
                () => PublishAsync(viewsStorage, outputKey, targetPath, result.Tables, cancellationToken),
                logger);

            logger.LogInformation(
                "Wrote SQLite view {ViewKey} from {StagingKey} with {TableCount} table(s): {RowCount} rows",
                outputKey, database.Key, result.Tables.Count, result.Tables.Sum(table => table.RowCount));

            return new SqliteExportFile
            {
                RunId = database.RunId,
                Key = outputKey,
                SourceTimestamp = database.SourceTimestamp,
                Tables = result.Tables,
                Created = true
            };
        }
        finally
        {
            Directory.Delete(workingDirectory, recursive: true);
        }
    }

    /// <summary>Present is not enough: an export produced by an earlier build of the transformation
    /// has to be rebuilt, or a fix to the script could never take effect for a source timestamp that
    /// has already been exported once.</summary>
    private async Task<IReadOnlyList<SqliteViewTable>?> GetExistingTablesAsync(
        IBlobStorageService viewsStorage,
        string outputKey,
        CancellationToken cancellationToken)
    {
        if (!await viewsStorage.ExistsAsync(outputKey, cancellationToken))
        {
            return null;
        }

        var metadata = await viewsStorage.GetMetadataAsync(outputKey, cancellationToken);
        var version = MetadataValue(metadata.UserMetadata, ViewsFileNaming.VersionMetadataKey);

        if (version != SqliteViewDefinition.Version)
        {
            logger.LogInformation(
                "SQLite view {ViewKey} was built by transformation {ExistingVersion}, not {Version}; rebuilding it",
                outputKey, version ?? "(unrecorded)", SqliteViewDefinition.Version);

            return null;
        }

        var tables = new List<SqliteViewTable>(SqliteViewDefinition.TableNames.Count);

        foreach (var tableName in SqliteViewDefinition.TableNames)
        {
            var count = MetadataValue(
                metadata.UserMetadata,
                ViewsFileNaming.TableCountMetadataKey(tableName));

            if (!long.TryParse(count, NumberStyles.None, CultureInfo.InvariantCulture, out var rowCount))
            {
                logger.LogInformation(
                    "SQLite view {ViewKey} has no row count for {TableName}; rebuilding it",
                    outputKey, tableName);

                return null;
            }

            tables.Add(new SqliteViewTable(tableName, rowCount));
        }

        return tables;
    }

    /// <summary>S3 hands user metadata back with a provider prefix on the key, the file system does
    /// not, so the key is matched by suffix rather than equality.</summary>
    private static string? MetadataValue(
        IReadOnlyDictionary<string, string> metadata,
        string key)
        => metadata
            .FirstOrDefault(entry => entry.Key.EndsWith(key, StringComparison.OrdinalIgnoreCase))
            .Value;

    private static async Task PublishAsync(
        IBlobStorageService viewsStorage,
        string outputKey,
        string localPath,
        IReadOnlyList<SqliteViewTable> tables,
        CancellationToken cancellationToken)
    {
        var metadata = new Dictionary<string, string>
        {
            [ViewsFileNaming.VersionMetadataKey] = SqliteViewDefinition.Version
        };

        foreach (var table in tables)
        {
            metadata[ViewsFileNaming.TableCountMetadataKey(table.Name)] =
                table.RowCount.ToString(CultureInfo.InvariantCulture);
        }

        await using var published = await viewsStorage.OpenWriteAsync(
            outputKey, ViewsFileNaming.DatabaseContentType, metadata, cancellationToken: cancellationToken);

        await using var built = new FileStream(localPath, FileMode.Open, FileAccess.Read);

        await built.CopyToAsync(published, cancellationToken);
    }

    private static async Task DownloadAsync(
        IBlobStorageService stagingStorage,
        string key,
        string localPath,
        CancellationToken cancellationToken)
    {
        await using var reader = await stagingStorage.OpenReadAsync(key, cancellationToken);
        await using var local = File.Create(localPath);

        await reader.CopyToAsync(local, cancellationToken);
    }
}
