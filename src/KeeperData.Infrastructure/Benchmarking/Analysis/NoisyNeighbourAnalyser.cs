using KeeperData.Infrastructure.Benchmarking.Models;

namespace KeeperData.Infrastructure.Benchmarking.Analysis;

/// <summary>
/// Analyses a <see cref="BenchmarkReport"/> for signs of shared-resource
/// contention ("noisy neighbour").  Each check has a threshold that a
/// healthy, uncontested local MongoDB instance should comfortably pass.
/// When the same benchmark is run against a contested production instance,
/// one or more flags should fire — making the comparison obvious.
/// </summary>
public static class NoisyNeighbourAnalyser
{
    /// <summary>Read-path command names eligible for tail-ratio analysis.</summary>
    private static readonly HashSet<string> s_readCommands = new(StringComparer.OrdinalIgnoreCase)
    {
        "find", "aggregate", "count", "distinct", "listCollections", "listIndexes"
    };

    public static NoisyNeighbourAnalysis Analyse(BenchmarkReport report)
    {
        var flags = new List<RedFlag>();

        CheckCheckoutWait(report.DriverMetrics, flags);
        CheckCheckoutFailures(report.DriverMetrics, flags);
        CheckPoolCleared(report.DriverMetrics, flags);
        CheckFindLatency(report.DriverMetrics, flags);
        CheckReadTailRatio(report.DriverMetrics, flags);
        CheckCommandFailures(report.DriverMetrics, flags);
        CheckScenarioErrors(report.ScenarioResults, flags);

        var cause = DiagnoseProbableCause(flags, report.DriverMetrics);

        return new NoisyNeighbourAnalysis
        {
            Flags = flags,
            ProbableCause = cause
        };
    }

    // ── Individual checks ─────────────────────────────────────────────
    // Each method is a single-concern check (SonarQube S138/S3776 friendly).

    private static void CheckCheckoutWait(DriverMetrics dm, List<RedFlag> flags)
    {
        if (dm.ConnectionCheckoutWait is not { } cw) return;

        if (cw.P95Ms > Thresholds.CheckoutWaitP95Ms)
        {
            flags.Add(Flag(
                "ConnectionPool.CheckoutWait.P95",
                RiskLevel.Warning,
                $"Connection checkout p95 is {cw.P95Ms}ms — pool is likely starved by concurrent consumers.",
                "Check maxPoolSize in MongoClientSettings; review co-located services sharing the same cluster.",
                cw.P95Ms, Thresholds.CheckoutWaitP95Ms));
        }

        if (cw.P99Ms > Thresholds.CheckoutWaitP99Ms)
        {
            flags.Add(Flag(
                "ConnectionPool.CheckoutWait.P99",
                RiskLevel.Critical,
                $"Connection checkout p99 is {cw.P99Ms}ms — severe pool contention detected.",
                "Increase maxPoolSize or reduce concurrent consumers. Consider a dedicated connection pool for the benchmark workload.",
                cw.P99Ms, Thresholds.CheckoutWaitP99Ms));
        }
    }

    private static void CheckCheckoutFailures(DriverMetrics dm, List<RedFlag> flags)
    {
        if (dm.CheckoutFailures <= Thresholds.CheckoutFailures) return;

        flags.Add(Flag(
            "ConnectionPool.CheckoutFailures",
            RiskLevel.Critical,
            $"{dm.CheckoutFailures} checkout failure(s) — connection pool was fully exhausted.",
            "Increase maxPoolSize and waitQueueTimeout, or reduce concurrent workloads against this cluster.",
            dm.CheckoutFailures, Thresholds.CheckoutFailures));
    }

    private static void CheckPoolCleared(DriverMetrics dm, List<RedFlag> flags)
    {
        if (dm.PoolClearedEvents <= Thresholds.PoolCleared) return;

        flags.Add(Flag(
            "ConnectionPool.PoolCleared",
            RiskLevel.Critical,
            $"{dm.PoolClearedEvents} pool-cleared event(s) — driver was forced to reset the connection pool.",
            "Investigate server-side errors, network timeouts, or primary stepdowns. Check mongod/mongos logs for election events.",
            dm.PoolClearedEvents, Thresholds.PoolCleared));
    }

    private static void CheckFindLatency(DriverMetrics dm, List<RedFlag> flags)
    {
        if (!dm.CommandLatency.TryGetValue("find", out var lat)) return;

        if (lat.P95Ms > Thresholds.FindP95Ms)
        {
            flags.Add(Flag(
                "CommandLatency.Find.P95",
                RiskLevel.Warning,
                $"Find p95 is {lat.P95Ms}ms — indexed lookups should be sub-{Thresholds.FindP95Ms}ms on an uncontested instance.",
                "Run .explain() on slow queries; check whether indexes are being evicted from the WiredTiger cache by other workloads.",
                lat.P95Ms, Thresholds.FindP95Ms));
        }

        if (lat.P99Ms > Thresholds.FindP99Ms)
        {
            flags.Add(Flag(
                "CommandLatency.Find.P99",
                RiskLevel.Critical,
                $"Find p99 is {lat.P99Ms}ms — high tail latency indicates intermittent resource contention.",
                "Correlate with db.serverStatus().wiredTiger.cache — high eviction rates indicate memory pressure from co-tenants.",
                lat.P99Ms, Thresholds.FindP99Ms));
        }
    }

    private static void CheckReadTailRatio(DriverMetrics dm, List<RedFlag> flags)
    {
        foreach (var (cmdName, lat) in dm.CommandLatency)
        {
            if (lat.P50Ms <= 0 || !s_readCommands.Contains(cmdName)) continue;

            var ratio = lat.P99Ms / lat.P50Ms;
            if (ratio <= Thresholds.TailLatencyRatio) continue;

            flags.Add(Flag(
                $"CommandLatency.{cmdName}.TailRatio",
                RiskLevel.Warning,
                $"'{cmdName}' p99/p50 ratio is {ratio:F1}x (p50={lat.P50Ms}ms, p99={lat.P99Ms}ms) — high jitter indicates contention.",
                "High jitter in reads is typically caused by periodic I/O stalls from checkpoint flushes or shared-disk contention. Check iowait and Mongo's slow query log.",
                Math.Round(ratio, 1), Thresholds.TailLatencyRatio));
        }
    }

    private static void CheckCommandFailures(DriverMetrics dm, List<RedFlag> flags)
    {
        var total = dm.CommandFailures.Values.Sum();
        if (total <= Thresholds.CommandFailures) return;

        var detail = string.Join(", ", dm.CommandFailures.Select(kv => $"{kv.Key}={kv.Value}"));
        flags.Add(Flag(
            "CommandFailures",
            RiskLevel.Critical,
            $"{total} command failure(s) ({detail}) — server is under pressure or timing out.",
            "Check server-side errors via db.currentOp() and mongod logs. Look for WriteConcernError, ExceededTimeLimit, or NetworkTimeout.",
            total, Thresholds.CommandFailures));
    }

    private static void CheckScenarioErrors(IReadOnlyList<ScenarioResult> scenarios, List<RedFlag> flags)
    {
        foreach (var s in scenarios)
        {
            if (s.TotalOperations == 0) continue;

            var rate = (double)s.ErrorCount / s.TotalOperations;
            if (rate <= Thresholds.ScenarioErrorRate) continue;

            flags.Add(Flag(
                $"Scenario.{s.ScenarioName}.ErrorRate",
                RiskLevel.Warning,
                $"Scenario '{s.ScenarioName}' error rate is {rate:P1} ({s.ErrorCount}/{s.TotalOperations}).",
                $"Review driver logs for the '{s.ScenarioName}' scenario to identify the error type (timeout, write conflict, etc.).",
                Math.Round(rate * 100, 1), Thresholds.ScenarioErrorRate * 100));
        }
    }

    // ── Cross-correlation ─────────────────────────────────────────────

    private static string? DiagnoseProbableCause(List<RedFlag> flags, DriverMetrics dm)
    {
        if (flags.Count == 0) return null;

        var categories = flags.Select(f => f.Category).ToHashSet(StringComparer.OrdinalIgnoreCase);

        // Pattern: pool cleared + checkout failures → connection storm / primary stepdown
        if (categories.Contains("ConnectionPool.PoolCleared") &&
            categories.Contains("ConnectionPool.CheckoutFailures"))
        {
            return "Connection pool was cleared AND checkout failures occurred. " +
                   "This pattern indicates a primary stepdown, network partition, or server crash " +
                   "that forced the driver to reset its connection pool. " +
                   "Check mongod logs for replica set election events.";
        }

        // Pattern: pool cleared + command failures → server-side error propagation
        if (categories.Contains("ConnectionPool.PoolCleared") &&
            categories.Contains("CommandFailures"))
        {
            return "Pool-cleared events combined with command failures suggest " +
                   "the server returned errors (e.g. ExceededTimeLimit, WriteConcernError) " +
                   "that triggered a pool reset. Investigate server-side resource limits " +
                   "(maxConns, oplogSize, disk IOPS).";
        }

        // Pattern: high checkout wait + high find latency → shared pool under load
        if (categories.Any(c => c.StartsWith("ConnectionPool.CheckoutWait")) &&
            categories.Any(c => c.StartsWith("CommandLatency.Find")))
        {
            return "Both connection checkout wait and find latency are elevated. " +
                   "This indicates the connection pool is saturated by other consumers " +
                   "sharing the same MongoDB cluster, causing queuing delays before " +
                   "commands even reach the server. " +
                   "Consider increasing maxPoolSize or isolating the workload to a dedicated cluster.";
        }

        // Pattern: high find latency only → cache/disk pressure
        if (categories.Any(c => c.StartsWith("CommandLatency.Find")))
        {
            return "Read latency is elevated without connection pool symptoms. " +
                   "This typically indicates WiredTiger cache pressure — other workloads " +
                   "are evicting hot pages, forcing reads to hit disk. " +
                   "Check db.serverStatus().wiredTiger.cache.pages_evicted_by_application_threads.";
        }

        // Pattern: checkout wait only → pool sizing issue
        if (categories.Any(c => c.StartsWith("ConnectionPool.CheckoutWait")))
        {
            return "Connection checkout wait is elevated but command latency is normal. " +
                   "The server is healthy but the driver's connection pool is undersized " +
                   "relative to the concurrency level. Increase maxPoolSize.";
        }

        // Pattern: command failures only → server-side pressure
        if (categories.Contains("CommandFailures"))
        {
            return "Command failures without connection pool issues suggest " +
                   "server-side resource exhaustion (CPU, disk IOPS, or oplog lag). " +
                   "Check db.currentOp() for long-running operations and mongod logs " +
                   "for slow query warnings.";
        }

        // Pattern: scenario errors only → application-level issue
        if (categories.Any(c => c.StartsWith("Scenario.")))
        {
            return "Scenario errors without driver-level symptoms suggest " +
                   "application-level issues (write conflicts, duplicate key errors). " +
                   "Review the scenario implementation and error details.";
        }

        return "One or more red flags were detected but the pattern does not match " +
               "a known root cause. Review individual flags for details.";
    }

    // ── Flag builder ──────────────────────────────────────────────────

    private static RedFlag Flag(
        string category,
        RiskLevel severity,
        string description,
        string remediation,
        double observed,
        double threshold) => new()
    {
        Category = category,
        Severity = severity,
        Description = description,
        Remediation = remediation,
        ObservedValue = observed,
        Threshold = threshold
    };

    // ── Thresholds ────────────────────────────────────────────────────

    /// <summary>
    /// Centralised thresholds.  Tuned so a local testcontainer / dev instance
    /// will NOT trigger them, but a resource-starved production Mongo will.
    /// </summary>
    internal static class Thresholds
    {
        internal const double CheckoutWaitP95Ms = 50;
        internal const double CheckoutWaitP99Ms = 100;
        internal const int CheckoutFailures = 0;
        internal const int PoolCleared = 0;
        internal const int CommandFailures = 0;
        internal const double FindP95Ms = 20;
        internal const double FindP99Ms = 50;
        internal const double TailLatencyRatio = 10;
        internal const double ScenarioErrorRate = 0.01;
    }
}
