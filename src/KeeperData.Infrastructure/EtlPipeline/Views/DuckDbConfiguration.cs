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

    /// <summary>Optionally overrides DuckDB's managed-memory limit, e.g. "1GB". Left unset,
    /// DuckDB uses 80% of the smaller of host memory and the Linux cgroup limit.</summary>
    public string? MemoryLimit { get; set; }
}
