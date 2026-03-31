namespace KeeperData.Infrastructure.Benchmarking.Throttling;

/// <summary>
/// Production throttler — injects a real <see cref="Task.Delay"/> between
/// operations so the benchmark does not saturate a shared MongoDB instance.
/// </summary>
public sealed class DefaultBenchmarkThrottler : IBenchmarkThrottler
{
    public async Task DelayAsync(TimeSpan delay, CancellationToken ct)
    {
        if (delay > TimeSpan.Zero)
        {
            await Task.Delay(delay, ct).ConfigureAwait(false);
        }
    }
}
