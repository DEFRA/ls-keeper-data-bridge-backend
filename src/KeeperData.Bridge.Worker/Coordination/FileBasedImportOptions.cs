namespace KeeperData.Bridge.Worker.Coordination;

/// <summary>
/// Configuration for the file-based ETL import trigger. Bound from the "FileBasedImport" section.
/// </summary>
public sealed class FileBasedImportOptions
{
    public const string SectionName = "FileBasedImport";

    /// <summary>
    /// Name of the distributed lock guarding a file-based run. Deliberately not the legacy
    /// "ImportRun": the two pipelines write disjoint folders, so making a legacy import block a
    /// file-based one buys nothing and would leave QA waiting hours. This lock still gives the
    /// one-file-based-run-at-a-time guarantee.
    /// </summary>
    public string LockName { get; set; } = "FileBasedEtlRun";

    public TimeSpan LockDuration { get; set; } = TimeSpan.FromMinutes(4);

    public TimeSpan RenewalInterval { get; set; } = TimeSpan.FromMinutes(1);

    public TimeSpan RenewalExtension { get; set; } = TimeSpan.FromMinutes(2);
}
