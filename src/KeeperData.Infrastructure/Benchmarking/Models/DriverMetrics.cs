namespace KeeperData.Infrastructure.Benchmarking.Models;

/// <summary>
/// Aggregated statistics captured from the MongoDB driver's command and
/// connection-pool events during a benchmark run.
/// </summary>
public sealed record DriverMetrics
{
    /// <summary>Per-command-type latency breakdown (find, insert, update, aggregate, etc.).</summary>
    public IReadOnlyDictionary<string, LatencyStats> CommandLatency { get; init; }
        = new Dictionary<string, LatencyStats>();

    /// <summary>Total count of failed commands by command name.</summary>
    public IReadOnlyDictionary<string, int> CommandFailures { get; init; }
        = new Dictionary<string, int>();

    /// <summary>Connection checkout wait-time statistics.</summary>
    public LatencyStats? ConnectionCheckoutWait { get; init; }

    /// <summary>Number of times a checkout from the connection pool failed.</summary>
    public int CheckoutFailures { get; init; }

    /// <summary>Connections opened during the benchmark.</summary>
    public int ConnectionsCreated { get; init; }

    /// <summary>Connections closed during the benchmark.</summary>
    public int ConnectionsClosed { get; init; }

    /// <summary>Number of pool-cleared events observed.</summary>
    public int PoolClearedEvents { get; init; }
}
