using KeeperData.Core.Pipeline;

namespace KeeperData.Core.ETL.Export.Pipeline;

/// <summary>
/// Run context for the CPH export pipeline. The source stage resolves and records the DuckDB key it
/// read from; the sink stage records the <see cref="CphExportResult"/> so the caller can read it back
/// after <see cref="IPipelineExecutor"/> completes (the executor itself returns no value).
/// </summary>
public sealed class CphExportContext : IPipelineContext
{
    /// <summary>
    /// The DuckDB source key to export from. When null the source stage resolves the latest staging
    /// file and writes the resolved key back here for the sink to use.
    /// </summary>
    public string? SourceDuckDbKey { get; set; }

    /// <summary>Set by the sink stage on success. Read by the caller after the run completes.</summary>
    public CphExportResult? Result { get; set; }
}
