namespace KeeperData.Infrastructure.Benchmarking.Throttling;

/// <summary>
/// Controls the delay injected between benchmark operations.
/// <para>
/// In production, the default implementation pauses between operations to
/// limit impact on co-located services.  In test contexts a no-op
/// implementation is supplied so benchmarks run at full speed.
/// </para>
/// </summary>
public interface IBenchmarkThrottler
{
    /// <summary>
    /// Wait for the configured throttle period.
    /// Implementations may return immediately (no-op) or honour
    /// the <paramref name="delay"/> value from the benchmark config.
    /// </summary>
    Task DelayAsync(TimeSpan delay, CancellationToken ct);
}
