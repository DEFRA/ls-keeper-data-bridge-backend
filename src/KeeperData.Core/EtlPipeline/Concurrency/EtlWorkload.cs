namespace KeeperData.Core.EtlPipeline.Concurrency;

/// <summary>Which budget a unit of work is charged against.</summary>
public enum EtlWorkload
{
    /// <summary>Charged against nothing. For fanning out over datasets, where the work itself
    /// happens in a nested per-file loop that holds the permits: an outer task that held a permit
    /// while waiting for an inner one could consume the budget and deadlock against itself.</summary>
    Unbounded,

    /// <summary>Spends its time waiting on storage rather than on a core.</summary>
    Io,

    /// <summary>Occupies a core, and a thread pool thread with it.</summary>
    Cpu
}
