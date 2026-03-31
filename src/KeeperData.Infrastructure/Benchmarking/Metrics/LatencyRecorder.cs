using KeeperData.Infrastructure.Benchmarking.Models;
using System.Collections.Concurrent;

namespace KeeperData.Infrastructure.Benchmarking.Metrics;

/// <summary>
/// Thread-safe helper that records durations and computes percentile statistics.
/// </summary>
public sealed class LatencyRecorder
{
    private readonly ConcurrentBag<double> _samples = [];

    public void Record(TimeSpan duration) => _samples.Add(duration.TotalMilliseconds);

    public int Count => _samples.Count;

    public LatencyStats Compute()
    {
        if (_samples.IsEmpty)
        {
            return new LatencyStats();
        }

        var sorted = _samples.OrderBy(x => x).ToArray();
        return new LatencyStats
        {
            AvgMs = Math.Round(sorted.Average(), 2),
            P50Ms = Math.Round(Percentile(sorted, 50), 2),
            P95Ms = Math.Round(Percentile(sorted, 95), 2),
            P99Ms = Math.Round(Percentile(sorted, 99), 2),
            MinMs = Math.Round(sorted[0], 2),
            MaxMs = Math.Round(sorted[^1], 2)
        };
    }

    private static double Percentile(double[] sorted, double p)
    {
        var index = (p / 100.0) * (sorted.Length - 1);
        var lower = (int)Math.Floor(index);
        var upper = (int)Math.Ceiling(index);
        if (lower == upper) return sorted[lower];
        return sorted[lower] + (index - lower) * (sorted[upper] - sorted[lower]);
    }
}
