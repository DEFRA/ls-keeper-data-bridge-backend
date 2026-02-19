namespace KeeperData.Core.Reports.Initialisation;

/// <summary>
/// Handles one-time initialisation tasks for the cleanse reports module
/// (e.g. ensuring MongoDB indexes exist).
/// Idempotent — safe to call multiple times.
/// </summary>
public interface ICleanseReportInitialisation
{
    Task InitialiseAsync(CancellationToken ct = default);
}
