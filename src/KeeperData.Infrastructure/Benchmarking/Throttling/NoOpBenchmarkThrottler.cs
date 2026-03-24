namespace KeeperData.Infrastructure.Benchmarking.Throttling;

/// <summary>
/// Test throttler — always returns immediately so performance tests
/// measure pure Mongo throughput without artificial delays.
/// </summary>
public sealed class NoOpBenchmarkThrottler : IBenchmarkThrottler
{
    public Task DelayAsync(TimeSpan delay, CancellationToken ct) => Task.CompletedTask;
}
