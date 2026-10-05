using DuckDB.NET.Data;

namespace KeeperData.Etl.Tool.Setup;

/// <summary>Finds the DuckDB SQLite extension the export stage loads.
///
/// Production bundles it into the image and configures the path; a desktop run has neither, and
/// autoloading is off in the writer because the production task cannot reach the network. So the
/// path is resolved here, from the version and platform DuckDB itself reports - an extension built
/// for a different DuckDB will not load.</summary>
internal static class SqliteExtensionResolver
{
    private const string FileName = "sqlite_scanner.duckdb_extension";

    public static string? Resolve(string? configured)
    {
        if (!string.IsNullOrWhiteSpace(configured)) return configured;

        var environment = Environment.GetEnvironmentVariable("DUCKDB_SQLITE_EXTENSION_PATH");

        if (!string.IsNullOrWhiteSpace(environment) && File.Exists(environment)) return environment;

        var (version, platform) = Build();

        return Candidates(version, platform).FirstOrDefault(File.Exists);
    }

    /// <summary>Where a matching extension is likely to already be: the cache the test suite fills,
    /// then DuckDB's own extension directory.</summary>
    private static IEnumerable<string> Candidates(string version, string platform)
    {
        yield return Path.Combine(Path.GetTempPath(), "krds-duckdb-extensions", version, platform, FileName);

        yield return Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
            ".duckdb", "extensions", version, platform, FileName);
    }

    private static (string Version, string Platform) Build()
    {
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT version(), (SELECT platform FROM pragma_platform())";

        using var reader = command.ExecuteReader();
        reader.Read();

        return (reader.GetString(0), reader.GetString(1));
    }
}
