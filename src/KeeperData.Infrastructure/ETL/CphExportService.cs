using DuckDB.NET.Data;
using KeeperData.Core.ETL.Export;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace KeeperData.Infrastructure.ETL;

public class CphExportService : ICphExportService
{
    private readonly IBlobStorageServiceFactory _storageFactory;
    private readonly ILogger<CphExportService> _logger;

    private const string StagingPrefix = "staging/";
    private const string ViewsPrefix = "views/";
    private const string DuckDbFilePattern = "keeper_data_bridge_";
    private const string DuckDbExtension = ".duckdb";

    public CphExportService(
        IBlobStorageServiceFactory storageFactory,
        ILogger<CphExportService> logger)
    {
        _storageFactory = storageFactory;
        _logger = logger;
    }

    public async Task<CphExportResult> ExportAsync(CancellationToken cancellationToken = default)
    {
        var latestDuckDbKey = await FindLatestDuckDbKeyAsync(cancellationToken);
        return await ExportAsync(latestDuckDbKey, cancellationToken);
    }

    public async Task<CphExportResult> ExportAsync(string sourceDuckDbKey, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting CPH export from DuckDB source: {SourceKey}", sourceDuckDbKey);

        var storageService = _storageFactory.GetSourceInternal();
        var tempDir = Path.Combine(Path.GetTempPath(), "cph-export", Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);

        try
        {
            var duckDbPath = Path.Combine(tempDir, "source.duckdb");
            await DownloadToFileAsync(storageService, sourceDuckDbKey, duckDbPath, cancellationToken);

            var cphValues = ReadDistinctCphs(duckDbPath);
            _logger.LogInformation("Extracted {Count} distinct CPH values from {SourceKey}", cphValues.Count, sourceDuckDbKey);

            var sqlitePath = Path.Combine(tempDir, "cphs.sqlite");
            WriteSqlite(sqlitePath, cphValues);

            var timestamp = ExtractTimestamp(sourceDuckDbKey);
            var sqliteKey = $"{ViewsPrefix}cphs_{timestamp}.sqlite";

            var existsAlready = await storageService.ExistsAsync(sqliteKey, cancellationToken);
            if (existsAlready)
            {
                _logger.LogInformation("SQLite file {SqliteKey} already exists — skipping upload (idempotent)", sqliteKey);
            }
            else
            {
                var sqliteBytes = await File.ReadAllBytesAsync(sqlitePath, cancellationToken);
                await storageService.UploadAsync(sqliteKey, sqliteBytes, "application/x-sqlite3", cancellationToken: cancellationToken);
                _logger.LogInformation("Uploaded SQLite file to {SqliteKey} ({RowCount} rows)", sqliteKey, cphValues.Count);
            }

            return new CphExportResult
            {
                SourceDuckDbKey = sourceDuckDbKey,
                SqliteKey = sqliteKey,
                RowCount = cphValues.Count,
                ExportedAt = DateTime.UtcNow
            };
        }
        finally
        {
            CleanupTempDirectory(tempDir);
        }
    }

    private async Task<string> FindLatestDuckDbKeyAsync(CancellationToken cancellationToken)
    {
        var storageService = _storageFactory.GetSourceInternal();
        var objects = await storageService.ListAsync(StagingPrefix, cancellationToken);

        var duckDbFiles = objects
            .Where(o => o.Key.Contains(DuckDbFilePattern) && o.Key.EndsWith(DuckDbExtension, StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(o => o.Key)
            .ToList();

        if (duckDbFiles.Count == 0)
        {
            throw new InvalidOperationException(
                $"No DuckDB staging files found matching pattern '{StagingPrefix}{DuckDbFilePattern}*{DuckDbExtension}'");
        }

        var latestKey = duckDbFiles[0].Key;
        _logger.LogInformation("Found latest DuckDB staging file: {Key}", latestKey);
        return latestKey;
    }

    private static async Task DownloadToFileAsync(
        IBlobStorageServiceReadOnly storageService,
        string objectKey,
        string localPath,
        CancellationToken cancellationToken)
    {
        await using var sourceStream = await storageService.OpenReadAsync(objectKey, cancellationToken);
        await using var fileStream = File.Create(localPath);
        await sourceStream.CopyToAsync(fileStream, cancellationToken);
    }

    internal static List<string> ReadDistinctCphs(string duckDbPath)
    {
        using var connection = new DuckDBConnection($"Data Source={duckDbPath}");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT DISTINCT CPH
            FROM sam_cph_holdings
            WHERE CPH IS NOT NULL AND CPH <> ''
            ORDER BY CPH
            """;

        var cphs = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            cphs.Add(reader.GetString(0));
        }

        return cphs;
    }

    internal static void WriteSqlite(string sqlitePath, List<string> cphValues)
    {
        using var connection = new SqliteConnection($"Data Source={sqlitePath}");
        connection.Open();

        using var createCmd = connection.CreateCommand();
        createCmd.CommandText = "CREATE TABLE cphs (CPH TEXT NOT NULL PRIMARY KEY)";
        createCmd.ExecuteNonQuery();

        using var transaction = connection.BeginTransaction();
        using var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO cphs (CPH) VALUES ($cph)";
        var param = insertCmd.Parameters.Add("$cph", SqliteType.Text);

        foreach (var cph in cphValues)
        {
            param.Value = cph;
            insertCmd.ExecuteNonQuery();
        }

        transaction.Commit();
        connection.Close();
        SqliteConnection.ClearPool(connection);
    }

    private static string ExtractTimestamp(string duckDbKey)
    {
        var fileName = Path.GetFileNameWithoutExtension(duckDbKey);
        var prefix = DuckDbFilePattern;
        var timestampIndex = fileName.IndexOf(prefix, StringComparison.Ordinal);
        if (timestampIndex < 0)
        {
            return DateTime.UtcNow.ToString("yyyyMMdd'T'HHmmss'Z'");
        }

        return fileName[(timestampIndex + prefix.Length)..];
    }

    private void CleanupTempDirectory(string tempDir)
    {
        try
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to clean up temp directory: {TempDir}", tempDir);
        }
    }
}
