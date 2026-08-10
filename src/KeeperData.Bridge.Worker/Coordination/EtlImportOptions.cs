namespace KeeperData.Bridge.Worker.Coordination;

/// <summary>
/// Configuration for the ETL import trigger. Bound from the "EtlImport" section.
/// </summary>
public sealed class EtlImportOptions
{
    public const string SectionName = "EtlImport";

    /// <summary>
    /// Name of the distributed lock guarding an ETL pipeline run. Deliberately not the legacy
    /// "ImportRun": the two pipelines write disjoint folders, so making a legacy import block an
    /// ETL run buys nothing and would leave QA waiting hours. This lock still gives the
    /// one-ETL-run-at-a-time guarantee.
    ///
    /// Changing this value is only safe while no instance is running the old one: two instances
    /// holding differently-named locks would each believe it had exclusive use of the pipeline.
    /// </summary>
    public string LockName { get; set; } = "EtlImportRun";

    public TimeSpan LockDuration { get; set; } = TimeSpan.FromMinutes(4);

    public TimeSpan RenewalInterval { get; set; } = TimeSpan.FromMinutes(1);

    public TimeSpan RenewalExtension { get; set; } = TimeSpan.FromMinutes(2);
}
