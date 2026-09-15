using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.EtlPipeline.Views;

/// <summary>DuckDB settings for the SQLite export stage.</summary>
[ExcludeFromCodeCoverage(Justification = "Configuration binding class - no logic to test.")]
public class DuckDbConfiguration
{
    /// <summary>The bundled SQLite extension. Loaded from disk because the task has no egress to
    /// DuckDB's extension repository; the file name must stay as published, because DuckDB derives
    /// the entry point it looks for from it.</summary>
    public string SqliteExtensionPath { get; set; } = "/opt/duckdb-extensions/sqlite_scanner.duckdb_extension";

    /// <summary>Caps DuckDB's working set, e.g. "1GB". Left unset, DuckDB sizes itself against the
    /// host rather than the container, which on a memory-limited task is the wrong number.</summary>
    public string? MemoryLimit { get; set; }
}
