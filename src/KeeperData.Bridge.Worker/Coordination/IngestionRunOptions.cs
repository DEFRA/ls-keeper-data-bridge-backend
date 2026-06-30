namespace KeeperData.Bridge.Worker.Coordination;

/// <summary>
/// Configuration for the ingestion run coordinator. Bound from the "IngestionRun" section.
/// Defaults reproduce the values the legacy task used, except the lock name (see <see cref="LockName"/>).
/// </summary>
public sealed class IngestionRunOptions
{
    public const string SectionName = "IngestionRun";

    /// <summary>
    /// Name of the distributed lock guarding a run. Default changed from the legacy
    /// "TaskProcessBulkFiles" to "ImportRun"; this only affects mutual exclusion across
    /// concurrent app instances (e.g. mid-deploy). Override here to keep the old value.
    /// </summary>
    public string LockName { get; set; } = "ImportRun";

    public TimeSpan LockDuration { get; set; } = TimeSpan.FromMinutes(4);

    public TimeSpan RenewalInterval { get; set; } = TimeSpan.FromMinutes(1);

    public TimeSpan RenewalExtension { get; set; } = TimeSpan.FromMinutes(2);
}
