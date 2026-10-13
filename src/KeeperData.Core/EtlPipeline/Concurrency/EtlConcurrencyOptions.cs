namespace KeeperData.Core.EtlPipeline.Concurrency;

/// <summary>Sizes the ETL's fan-out. Left unset, the budgets are derived from the cores the process
/// can actually see, which inside a container is the cgroup CPU quota rather than the host's cores.</summary>
public sealed class EtlConcurrencyOptions
{
    public const string SectionName = "EtlConcurrency";

    /// <summary>Cores held back from the CPU budget. The ETL runs in the same process as the web
    /// host, so saturating every core starves the thread pool that answers /health and the platform
    /// kills the task part way through a run.</summary>
    public int ReservedCores { get; set; } = 1;

    /// <summary>Overrides the derived CPU budget; null derives it from the visible cores.</summary>
    public int? CpuBudget { get; set; }

    /// <summary>Overrides the derived I/O budget; null derives it from the CPU budget.</summary>
    public int? IoBudget { get; set; }

    /// <summary>How far I/O-bound work may run ahead of the CPU budget. A decrypt spends most of its
    /// life waiting on storage, so more of them fit than there are cores to run them on.</summary>
    public int IoMultiplier { get; set; } = 4;

    /// <summary>Ceiling on the derived I/O budget, so a large host does not open so many concurrent
    /// transfers that they contend for bandwidth instead of using it.</summary>
    public int MaxIoBudget { get; set; } = 32;

    /// <summary>Thread pool threads kept warm beyond the CPU budget. The pool only grows by about
    /// one thread per second once it is saturated, which is long enough for a health probe queued
    /// behind a fan-out to time out.</summary>
    public int ReservedThreadPoolThreads { get; set; } = 16;
}
