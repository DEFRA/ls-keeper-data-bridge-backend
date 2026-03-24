namespace KeeperData.Infrastructure.Benchmarking.Models;

/// <summary>
/// Latency statistics computed from a collection of recorded durations.
/// </summary>
public sealed record LatencyStats
{
    public double AvgMs { get; init; }
    public double P50Ms { get; init; }
    public double P95Ms { get; init; }
    public double P99Ms { get; init; }
    public double MinMs { get; init; }
    public double MaxMs { get; init; }
}
