namespace KeeperData.Infrastructure.Benchmarking.Models;

/// <summary>
/// Configuration for a benchmark run. All values have safe defaults
/// designed to avoid overloading a shared production database.
/// </summary>
public sealed record BenchmarkConfig
{
    /// <summary>Number of deterministic seed records to create.</summary>
    public int SeedCount { get; init; } = 10_000;

    /// <summary>Maximum degree of parallelism for scenario execution.</summary>
    public int Concurrency { get; init; } = 4;

    /// <summary>Total benchmark duration. Scenarios loop until this elapses.</summary>
    public TimeSpan Duration { get; init; } = TimeSpan.FromMinutes(3);

    /// <summary>
    /// Delay injected between every Mongo operation to throttle throughput
    /// and limit impact on co-located services.
    /// </summary>
    public TimeSpan ThrottleDelay { get; init; } = TimeSpan.FromMilliseconds(10);

    /// <summary>Prefix applied to all temporary benchmark collections.</summary>
    public string CollectionPrefix { get; init; } = "_benchmark_";
}
