using System.Runtime.CompilerServices;
using KeeperData.Core.ETL.Export.Pipeline;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;

namespace KeeperData.Infrastructure.ETL.Pipeline;

/// <summary>
/// Source stage: resolves the DuckDB staging file (latest, or the key supplied on the context),
/// downloads it, and streams the distinct CPH values out. The resolved key is written back to the
/// context so the sink can derive the SQLite output key.
/// </summary>
public sealed class DuckDbCphSource : ISourceStage<string>
{
    private readonly IBlobStorageServiceFactory _storageFactory;
    private readonly ILogger<DuckDbCphSource> _logger;

    private const string StagingPrefix = "staging/";
    private const string DuckDbFilePattern = "keeper_data_bridge_";
    private const string DuckDbExtension = ".duckdb";

    public DuckDbCphSource(IBlobStorageServiceFactory storageFactory, ILogger<DuckDbCphSource> logger)
    {
        _storageFactory = storageFactory;
        _logger = logger;
    }

    public string Name => "duckdb-cph-source";

    public async IAsyncEnumerable<string> RunAsync(
        IPipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var exportContext = (CphExportContext)context;
        var storageService = _storageFactory.GetSourceInternal();

        var sourceKey = exportContext.SourceDuckDbKey
            ?? await FindLatestDuckDbKeyAsync(storageService, cancellationToken);
        exportContext.SourceDuckDbKey = sourceKey;

        _logger.LogInformation("Starting CPH export from DuckDB source: {SourceKey}", sourceKey);

        var tempDir = Path.Combine(Path.GetTempPath(), "cph-export", Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);

        try
        {
            var duckDbPath = Path.Combine(tempDir, "source.duckdb");
            await using (var sourceStream = await storageService.OpenReadAsync(sourceKey, cancellationToken))
            await using (var fileStream = File.Create(duckDbPath))
            {
                await sourceStream.CopyToAsync(fileStream, cancellationToken);
            }

            var cphValues = CphExportService.ReadDistinctCphs(duckDbPath);
            _logger.LogInformation("Extracted {Count} distinct CPH values from {SourceKey}", cphValues.Count, sourceKey);

            foreach (var cph in cphValues)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return cph;
            }
        }
        finally
        {
            CleanupTempDirectory(tempDir);
        }
    }

    private async Task<string> FindLatestDuckDbKeyAsync(
        IBlobStorageServiceReadOnly storageService,
        CancellationToken cancellationToken)
    {
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
