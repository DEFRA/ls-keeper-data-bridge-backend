using KeeperData.Core;

namespace KeeperData.Etl.Tool.Configuration;
/// <summary>Where the tool reads from, where it writes, and the handful of settings the pipeline
/// cannot infer. Bound from the EtlTool section, overridden by environment variables, then by the
/// command line.</summary>
public sealed class EtlToolOptions
{
    public const string SectionName = "EtlTool";

    /// <summary>Which supplier bucket to read. Names the credential set: an environment of "prod"
    /// reads S3PROD_AWS_ACCESS_KEY_ID, S3PROD_AWS_SECRET_ACCESS_KEY, S3PROD_BUCKET_NAME and
    /// S3PROD_ENDPOINTURL.</summary>
    public string Environment { get; set; } = DefaultEnvironment;

    /// <summary>Every interstitial artefact and the SQLite read model, under one root: raw,
    /// normalised, optimised, snapshots, staging and views. Only the source is remote.</summary>
    public string StagingPath { get; set; } = DefaultStagingPath;

    /// <summary>The vendored DuckDB SQLite extension. The export stage cannot autoload one, so this
    /// must point at a file already on disk; left unset, the tool resolves a version match itself.</summary>
    public string? SqliteExtensionPath { get; set; }

    /// <summary>Caps DuckDB's memory, e.g. "4GB". Null leaves it to DuckDB.</summary>
    public string? MemoryLimit { get; set; }

    public const string DefaultStagingPath = @"C:\livestock\krds-etl-data";

    public const string DefaultEnvironment = "prod";

    /// <summary>The run history, beside the artefacts it describes, so copying the staging folder
    /// takes the history with it.</summary>
    public string HistoryDatabasePath => Path.Combine(StagingPath, "etl-history.db");

    public string AccessKeyVariable => $"{Prefix}_AWS_ACCESS_KEY_ID";

    public string SecretKeyVariable => $"{Prefix}_AWS_SECRET_ACCESS_KEY";

    public string BucketVariable => $"{Prefix}_BUCKET_NAME";

    public string EndpointVariable => $"{Prefix}_ENDPOINTURL";

    private string Prefix => $"S3{Environment.ToUpperInvariant()}";
}
