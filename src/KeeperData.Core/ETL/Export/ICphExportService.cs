namespace KeeperData.Core.ETL.Export;

/// <summary>
/// Exports distinct CPH values from a DuckDB staging file to a SQLite database
/// and uploads the result to S3.
/// </summary>
public interface ICphExportService
{
    /// <summary>
    /// Runs the CPH export: reads the latest DuckDB from the staging prefix,
    /// extracts distinct CPH values, writes them to a SQLite file, and uploads
    /// the SQLite file to the views prefix in S3.
    /// </summary>
    /// <returns>The result of the export operation.</returns>
    Task<CphExportResult> ExportAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Runs the CPH export against a specific DuckDB source key.
    /// </summary>
    Task<CphExportResult> ExportAsync(string sourceDuckDbKey, CancellationToken cancellationToken = default);
}
