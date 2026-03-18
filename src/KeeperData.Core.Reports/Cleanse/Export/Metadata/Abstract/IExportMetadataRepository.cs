namespace KeeperData.Core.Reports.Cleanse.Export.Metadata.Abstract;

/// <summary>
/// Repository for reading and writing export metadata (last successful export timestamp).
/// </summary>
public interface IExportMetadataRepository
{
    /// <summary>
    /// Gets the UTC timestamp of the last successful incremental export, or null if none has occurred.
    /// </summary>
    Task<DateTime?> GetLastExportedAtUtcAsync(CancellationToken ct = default);

    /// <summary>
    /// Records the UTC timestamp of a successful incremental export.
    /// </summary>
    Task SetLastExportedAtUtcAsync(DateTime exportedAtUtc, CancellationToken ct = default);
}
