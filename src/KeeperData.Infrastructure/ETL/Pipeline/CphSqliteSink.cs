using KeeperData.Core.ETL.Export;
using KeeperData.Core.ETL.Export.Pipeline;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;

namespace KeeperData.Infrastructure.ETL.Pipeline;

/// <summary>
/// Sink stage: collects the streamed CPH values, writes them to a SQLite file, and uploads it to the
/// views prefix (idempotent — skips the upload when the key already exists). The resulting
/// <see cref="CphExportResult"/> is both returned and written to the context for the caller to read.
/// </summary>
public sealed class CphSqliteSink : AggregateStage<string, CphExportResult>
{
    private readonly IBlobStorageServiceFactory _storageFactory;
    private readonly ILogger<CphSqliteSink> _logger;

    private const string ViewsPrefix = "views/";
    private const string DuckDbFilePattern = "keeper_data_bridge_";

    public CphSqliteSink(IBlobStorageServiceFactory storageFactory, ILogger<CphSqliteSink> logger)
    {
        _storageFactory = storageFactory;
        _logger = logger;
    }

    public override string Name => "cph-sqlite-sink";

    protected override async Task<CphExportResult> AggregateAsync(
        IReadOnlyList<string> all,
        IPipelineContext context,
        CancellationToken cancellationToken)
    {
        var exportContext = (CphExportContext)context;
        var sourceKey = exportContext.SourceDuckDbKey
            ?? throw new InvalidOperationException("Source DuckDB key was not set by the source stage.");

        var storageService = _storageFactory.GetSourceInternal();
        var tempDir = Path.Combine(Path.GetTempPath(), "cph-export", Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);

        try
        {
            var sqlitePath = Path.Combine(tempDir, "cphs.sqlite");
            CphExportService.WriteSqlite(sqlitePath, [.. all]);

            var timestamp = ExtractTimestamp(sourceKey);
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
                _logger.LogInformation("Uploaded SQLite file to {SqliteKey} ({RowCount} rows)", sqliteKey, all.Count);
            }

            var result = new CphExportResult
            {
                SourceDuckDbKey = sourceKey,
                SqliteKey = sqliteKey,
                RowCount = all.Count,
                ExportedAt = DateTime.UtcNow
            };

            exportContext.Result = result;
            return result;
        }
        finally
        {
            CleanupTempDirectory(tempDir);
        }
    }

    private static string ExtractTimestamp(string duckDbKey)
    {
        var fileName = Path.GetFileNameWithoutExtension(duckDbKey);
        var timestampIndex = fileName.IndexOf(DuckDbFilePattern, StringComparison.Ordinal);
        if (timestampIndex < 0)
        {
            return DateTime.UtcNow.ToString("yyyyMMdd'T'HHmmss'Z'");
        }

        return fileName[(timestampIndex + DuckDbFilePattern.Length)..];
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
