namespace KeeperData.Core.EtlPipeline.Views;

/// <summary>The DuckDB SQLite extension could not be loaded.
///
/// The underlying failure is a DuckDB IO error that reads as though the file were corrupt. In
/// practice there are three causes: the extension was not bundled into the image, it was renamed
/// (DuckDB derives the entry point it looks for from the file name), or it was built for a different
/// DuckDB version from the one the application links against.
///
/// Downloading it at run time is not a fallback: the task has no egress. The configured path is
/// deliberately absent from the message, which is served to API callers; it is logged instead.</summary>
public sealed class SqliteViewExtensionException(Exception innerException)
    : Exception(Explanation, innerException), IEtlDiagnosableException
{
    private const string Explanation =
        "Could not load the DuckDB SQLite extension. It must be bundled into the image at the configured " +
        "path, keep its original file name, and be built for the same DuckDB version as the application.";
}
