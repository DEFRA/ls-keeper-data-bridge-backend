namespace KeeperData.Infrastructure.Benchmarking.Models;

/// <summary>
/// Result of a single benchmark scenario.
/// </summary>
public sealed record ScenarioResult
{
    public string ScenarioName { get; init; } = default!;
    public int TotalOperations { get; init; }
    public int ErrorCount { get; init; }
    public double ElapsedSeconds { get; init; }
    public double OpsPerSecond { get; init; }

    /// <summary>
    /// Effective throughput excluding throttle wait time.
    /// Gives a pure measure of Mongo performance independent of the throttle setting.
    /// </summary>
    public double EffectiveOpsPerSecond { get; init; }

    public LatencyStats Latency { get; init; } = default!;
}
