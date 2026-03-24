using KeeperData.Infrastructure.Benchmarking.Models;
using KeeperData.Infrastructure.Benchmarking.Throttling;

namespace KeeperData.Infrastructure.Benchmarking.Scenarios;

/// <summary>
/// A single, self-contained benchmark scenario that runs operations in a
/// tight loop for the configured duration.
/// </summary>
public interface IBenchmarkScenario
{
    string Name { get; }

    /// <summary>
    /// Execute the scenario, returning aggregated results.
    /// Implementations must honour <paramref name="ct"/> and throttle
    /// via the supplied <paramref name="throttler"/>.
    /// </summary>
    Task<ScenarioResult> RunAsync(BenchmarkConfig config, IBenchmarkThrottler throttler, CancellationToken ct);
}
