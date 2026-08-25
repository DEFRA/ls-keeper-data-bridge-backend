namespace KeeperData.Core.EtlPipeline.Storage;

/// <summary>The top-level folders the ETL pipeline materialises into.
/// Each is a top-level folder at the root of the internal bucket, a sibling of the
/// legacy ETL folders. The legacy ETL output folders are not represented here and
/// are not touched by this pipeline.</summary>
public static class EtlPipelineFolders
{
    /// <summary>Decrypted "csv" data files. In practice these are PSV, plus the legacy
    /// H/C/D/T formats. Materialised by the decrypt stage.</summary>
    public const string Raw = "raw";

    /// <summary>Parquet files transformed from the PSV and legacy H/C/D/T PSV formats.
    /// Materialised by the normalise stage.</summary>
    public const string Normalised = "normalised";

    /// <summary>Normalised parquet files reprocessed into snapshots by walking the deltas.
    /// Materialised by the snapshot stage.</summary>
    public const string Snapshots = "snapshots";

    /// <summary>The DuckDB database that the parquet files are loaded into.
    /// Materialised by the load stage.</summary>
    public const string Staging = "staging";

    /// <summary>SQLite exports of the DuckDB views, for use by the Keeper Data API.</summary>
    public const string Views = "views";
}
