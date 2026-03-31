using KeeperData.Infrastructure.Benchmarking.Models;

namespace KeeperData.Infrastructure.Benchmarking.Services;

/// <summary>
/// Runs a self-contained MongoDB benchmark, collecting driver metrics and explain plans,
/// then tears down all temporary collections.
/// </summary>
public interface IBenchmarkOrchestrator
{
    /// <summary>
    /// Start a benchmark run. Only one run is allowed at a time.
    /// Returns <c>null</c> if a run is already in progress.
    /// </summary>
    Task<BenchmarkReport?> RunAsync(BenchmarkConfig config, CancellationToken ct);

    /// <summary>
    /// Returns the report from the last completed (or cancelled) run, or <c>null</c>.
    /// </summary>
    BenchmarkReport? LastReport { get; }

    /// <summary>
    /// Returns <c>true</c> when a benchmark is currently executing.
    /// </summary>
    bool IsRunning { get; }
}
