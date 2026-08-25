using System.IO.Compression;
using DuckDB.NET.Data;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.Views;

/// <summary>Finds the DuckDB SQLite extension for these tests.
///
/// Production bundles the extension into the image; a test run has no image, so it resolves one the
/// same way the image build does - from the DuckDB version the package links against - and caches it.
/// Set DUCKDB_SQLITE_EXTENSION_PATH to point at a copy instead and nothing is downloaded.
///
/// The file name is the published one on purpose: DuckDB derives the entry point it looks for from
/// it, so a renamed extension fails to load.</summary>
public static class DuckDbSqliteExtension
{
    private const string FileName = "sqlite_scanner.duckdb_extension";

    private static readonly Lazy<string> Resolved = new(Resolve, LazyThreadSafetyMode.ExecutionAndPublication);

    public static string Path => Resolved.Value;

    private static string Resolve()
    {
        var configured = Environment.GetEnvironmentVariable("DUCKDB_SQLITE_EXTENSION_PATH");

        if (!string.IsNullOrWhiteSpace(configured) && File.Exists(configured))
        {
            return configured;
        }

        var (version, platform) = DuckDbBuild();

        var cached = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(), "krds-duckdb-extensions", version, platform, FileName);

        if (File.Exists(cached))
        {
            return cached;
        }

        Directory.CreateDirectory(System.IO.Path.GetDirectoryName(cached)!);
        Download($"https://extensions.duckdb.org/{version}/{platform}/{FileName}.gz", cached);

        return cached;
    }

    /// <summary>The extension has to match the running DuckDB exactly, so both halves of the address
    /// come from DuckDB itself rather than from anything written down here.</summary>
    private static (string Version, string Platform) DuckDbBuild()
    {
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT version(), (SELECT platform FROM pragma_platform())";
        using var reader = command.ExecuteReader();
        reader.Read();

        return (reader.GetString(0), reader.GetString(1));
    }

    private static void Download(string url, string destination)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromMinutes(5) };

        using var response = client.GetAsync(url).GetAwaiter().GetResult();
        response.EnsureSuccessStatusCode();

        using var compressed = response.Content.ReadAsStreamAsync().GetAwaiter().GetResult();
        using var decompressed = new GZipStream(compressed, CompressionMode.Decompress);

        var partial = destination + ".partial";

        using (var file = File.Create(partial))
        {
            decompressed.CopyTo(file);
        }

        File.Move(partial, destination, overwrite: true);
    }
}
