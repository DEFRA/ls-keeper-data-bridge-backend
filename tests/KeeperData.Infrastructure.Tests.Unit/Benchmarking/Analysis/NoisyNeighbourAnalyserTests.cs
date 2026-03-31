using FluentAssertions;
using KeeperData.Infrastructure.Benchmarking.Analysis;
using KeeperData.Infrastructure.Benchmarking.Models;

namespace KeeperData.Infrastructure.Tests.Unit.Benchmarking.Analysis;

public class NoisyNeighbourAnalyserTests
{
    private static LatencyStats Healthy(double avg = 0.5, double p50 = 0.4, double p95 = 1, double p99 = 2) =>
        new() { AvgMs = avg, P50Ms = p50, P95Ms = p95, P99Ms = p99, MinMs = 0.1, MaxMs = 3 };

    private static DriverMetrics HealthyDriverMetrics() => new()
    {
        CommandLatency = new Dictionary<string, LatencyStats>
        {
            ["find"] = Healthy(),
            ["update"] = Healthy(1, 0.8, 2, 4)
        },
        CommandFailures = new Dictionary<string, int>(),
        ConnectionCheckoutWait = Healthy(0.2, 0.1, 0.5, 1),
        CheckoutFailures = 0,
        ConnectionsCreated = 5,
        ConnectionsClosed = 0,
        PoolClearedEvents = 0
    };

    private static BenchmarkReport HealthyReport(
        DriverMetrics? dm = null,
        IReadOnlyList<ScenarioResult>? scenarios = null) => new()
    {
        Config = new BenchmarkConfig(),
        Status = "Completed",
        TotalElapsedSeconds = 10,
        DriverMetrics = dm ?? HealthyDriverMetrics(),
        ScenarioResults = scenarios ?? new[]
        {
            new ScenarioResult { ScenarioName = "PointLookup", TotalOperations = 100, ErrorCount = 0, Latency = Healthy() }
        }
    };

    // ── Healthy baseline ──────────────────────────────────────────────

    [Fact]
    public void HealthyReport_HasNoRedFlags()
    {
        var result = NoisyNeighbourAnalyser.Analyse(HealthyReport());

        result.HasRedFlags.Should().BeFalse();
        result.OverallRisk.Should().Be(RiskLevel.None);
        result.ProbableCause.Should().BeNull();
        result.Flags.Should().BeEmpty();
    }

    // ── Connection pool checks ────────────────────────────────────────

    [Fact]
    public void CheckoutWaitP95_AboveThreshold_FlagsPoolStarvation()
    {
        var dm = HealthyDriverMetrics() with
        {
            ConnectionCheckoutWait = new LatencyStats { AvgMs = 30, P50Ms = 20, P95Ms = 60, P99Ms = 80, MinMs = 1, MaxMs = 100 }
        };

        var result = NoisyNeighbourAnalyser.Analyse(HealthyReport(dm));

        result.HasRedFlags.Should().BeTrue();
        var flag = result.Flags.Should().ContainSingle(f => f.Category == "ConnectionPool.CheckoutWait.P95").Subject;
        flag.Severity.Should().Be(RiskLevel.Warning);
        flag.Remediation.Should().NotBeNullOrWhiteSpace();
    }

    [Fact]
    public void CheckoutWaitP99_AboveThreshold_FlagsSevereContention()
    {
        var dm = HealthyDriverMetrics() with
        {
            ConnectionCheckoutWait = new LatencyStats { AvgMs = 50, P50Ms = 30, P95Ms = 40, P99Ms = 150, MinMs = 1, MaxMs = 200 }
        };

        var result = NoisyNeighbourAnalyser.Analyse(HealthyReport(dm));

        result.HasRedFlags.Should().BeTrue();
        var flag = result.Flags.Should().ContainSingle(f => f.Category == "ConnectionPool.CheckoutWait.P99").Subject;
        flag.Severity.Should().Be(RiskLevel.Critical);
    }

    [Fact]
    public void CheckoutFailures_FlagsPoolExhaustion()
    {
        var dm = HealthyDriverMetrics() with { CheckoutFailures = 3 };

        var result = NoisyNeighbourAnalyser.Analyse(HealthyReport(dm));

        result.HasRedFlags.Should().BeTrue();
        var flag = result.Flags.Should().ContainSingle(f => f.Category == "ConnectionPool.CheckoutFailures").Subject;
        flag.Severity.Should().Be(RiskLevel.Critical);
    }

    [Fact]
    public void PoolClearedEvents_FlagsPoolReset()
    {
        var dm = HealthyDriverMetrics() with { PoolClearedEvents = 1 };

        var result = NoisyNeighbourAnalyser.Analyse(HealthyReport(dm));

        result.HasRedFlags.Should().BeTrue();
        result.Flags.Should().ContainSingle(f => f.Category == "ConnectionPool.PoolCleared");
    }

    // ── Command latency checks ────────────────────────────────────────

    [Fact]
    public void FindP95_AboveThreshold_FlagsSlowLookups()
    {
        var dm = HealthyDriverMetrics() with
        {
            CommandLatency = new Dictionary<string, LatencyStats>
            {
                ["find"] = new() { AvgMs = 10, P50Ms = 5, P95Ms = 25, P99Ms = 40, MinMs = 0.5, MaxMs = 50 },
                ["update"] = Healthy(1, 0.8, 2, 4)
            }
        };

        var result = NoisyNeighbourAnalyser.Analyse(HealthyReport(dm));

        result.HasRedFlags.Should().BeTrue();
        result.Flags.Should().Contain(f => f.Category == "CommandLatency.Find.P95");
    }

    [Fact]
    public void FindP99_AboveThreshold_FlagsTailLatency()
    {
        var dm = HealthyDriverMetrics() with
        {
            CommandLatency = new Dictionary<string, LatencyStats>
            {
                ["find"] = new() { AvgMs = 5, P50Ms = 3, P95Ms = 15, P99Ms = 60, MinMs = 0.5, MaxMs = 80 },
                ["update"] = Healthy(1, 0.8, 2, 4)
            }
        };

        var result = NoisyNeighbourAnalyser.Analyse(HealthyReport(dm));

        result.HasRedFlags.Should().BeTrue();
        result.Flags.Should().Contain(f => f.Category == "CommandLatency.Find.P99");
    }

    [Fact]
    public void TailLatencyRatio_AboveThreshold_FlagsJitter()
    {
        var dm = HealthyDriverMetrics() with
        {
            CommandLatency = new Dictionary<string, LatencyStats>
            {
                ["find"] = new() { AvgMs = 2, P50Ms = 0.5, P95Ms = 10, P99Ms = 55, MinMs = 0.1, MaxMs = 100 },
                ["update"] = Healthy(1, 0.8, 2, 4)
            }
        };

        var result = NoisyNeighbourAnalyser.Analyse(HealthyReport(dm));

        result.HasRedFlags.Should().BeTrue();
        result.Flags.Should().Contain(f => f.Category == "CommandLatency.find.TailRatio");
    }

    [Fact]
    public void TailLatencyRatio_WriteCommands_AreExcluded()
    {
        var dm = HealthyDriverMetrics() with
        {
            CommandLatency = new Dictionary<string, LatencyStats>
            {
                ["find"] = Healthy(),
                ["update"] = new() { AvgMs = 5, P50Ms = 0.5, P95Ms = 20, P99Ms = 80, MinMs = 0.1, MaxMs = 100 },
                ["insert"] = new() { AvgMs = 5, P50Ms = 0.5, P95Ms = 20, P99Ms = 80, MinMs = 0.1, MaxMs = 100 }
            }
        };

        var result = NoisyNeighbourAnalyser.Analyse(HealthyReport(dm));

        result.Flags.Should().NotContain(f => f.Category.Contains("update.TailRatio"));
        result.Flags.Should().NotContain(f => f.Category.Contains("insert.TailRatio"));
    }

    // ── Command failure checks ────────────────────────────────────────

    [Fact]
    public void CommandFailures_FlagsServerPressure()
    {
        var dm = HealthyDriverMetrics() with
        {
            CommandFailures = new Dictionary<string, int> { ["find"] = 2, ["update"] = 1 }
        };

        var result = NoisyNeighbourAnalyser.Analyse(HealthyReport(dm));

        result.HasRedFlags.Should().BeTrue();
        var flag = result.Flags.Should().ContainSingle(f => f.Category == "CommandFailures").Subject;
        flag.ObservedValue.Should().Be(3);
        flag.Severity.Should().Be(RiskLevel.Critical);
    }

    // ── Scenario error checks ─────────────────────────────────────────

    [Fact]
    public void ScenarioErrorRate_AboveThreshold_FlagsErrors()
    {
        var scenarios = new[]
        {
            new ScenarioResult
            {
                ScenarioName = "BulkWrite",
                TotalOperations = 100,
                ErrorCount = 5,
                Latency = Healthy()
            }
        };

        var result = NoisyNeighbourAnalyser.Analyse(HealthyReport(scenarios: scenarios));

        result.HasRedFlags.Should().BeTrue();
        result.Flags.Should().Contain(f => f.Category == "Scenario.BulkWrite.ErrorRate");
    }

    // ── Cross-correlation / probable cause ─────────────────────────────

    [Fact]
    public void PoolCleared_And_CheckoutFailures_DiagnosesConnectionStorm()
    {
        var dm = HealthyDriverMetrics() with
        {
            CheckoutFailures = 2,
            PoolClearedEvents = 1
        };

        var result = NoisyNeighbourAnalyser.Analyse(HealthyReport(dm));

        result.ProbableCause.Should().Contain("primary stepdown");
    }

    [Fact]
    public void PoolCleared_And_CommandFailures_DiagnosesServerErrors()
    {
        var dm = HealthyDriverMetrics() with
        {
            PoolClearedEvents = 1,
            CommandFailures = new Dictionary<string, int> { ["find"] = 3 }
        };

        var result = NoisyNeighbourAnalyser.Analyse(HealthyReport(dm));

        result.ProbableCause.Should().Contain("server returned errors");
    }

    [Fact]
    public void HighCheckoutWait_And_HighFindLatency_DiagnosesSharedPool()
    {
        var dm = HealthyDriverMetrics() with
        {
            ConnectionCheckoutWait = new LatencyStats { AvgMs = 30, P50Ms = 20, P95Ms = 60, P99Ms = 80, MinMs = 1, MaxMs = 100 },
            CommandLatency = new Dictionary<string, LatencyStats>
            {
                ["find"] = new() { AvgMs = 10, P50Ms = 5, P95Ms = 25, P99Ms = 40, MinMs = 0.5, MaxMs = 50 },
                ["update"] = Healthy()
            }
        };

        var result = NoisyNeighbourAnalyser.Analyse(HealthyReport(dm));

        result.ProbableCause.Should().Contain("connection pool is saturated");
    }

    [Fact]
    public void HighFindLatency_Only_DiagnosesCachePressure()
    {
        var dm = HealthyDriverMetrics() with
        {
            CommandLatency = new Dictionary<string, LatencyStats>
            {
                ["find"] = new() { AvgMs = 10, P50Ms = 5, P95Ms = 25, P99Ms = 60, MinMs = 0.5, MaxMs = 80 },
                ["update"] = Healthy()
            }
        };

        var result = NoisyNeighbourAnalyser.Analyse(HealthyReport(dm));

        result.ProbableCause.Should().Contain("WiredTiger cache pressure");
    }

    [Fact]
    public void OverallRisk_ReflectsHighestSeverity()
    {
        var dm = HealthyDriverMetrics() with
        {
            CheckoutFailures = 2,
            PoolClearedEvents = 1,
            CommandFailures = new Dictionary<string, int> { ["find"] = 1 }
        };

        var result = NoisyNeighbourAnalyser.Analyse(HealthyReport(dm));

        result.HasRedFlags.Should().BeTrue();
        result.OverallRisk.Should().Be(RiskLevel.Critical);
        result.Flags.Count.Should().BeGreaterThanOrEqualTo(3);
    }
}
