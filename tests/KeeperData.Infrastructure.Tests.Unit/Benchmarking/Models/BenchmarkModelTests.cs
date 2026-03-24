using FluentAssertions;
using KeeperData.Infrastructure.Benchmarking.Models;

namespace KeeperData.Infrastructure.Tests.Unit.Benchmarking.Models;

public class BenchmarkReportTests
{
    [Fact]
    public void DefaultValues_ArePopulated()
    {
        var report = new BenchmarkReport();

        report.Environment.Should().NotBeNullOrEmpty();
        report.TimestampUtc.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));
        report.DatasetFingerprints.Should().BeEmpty();
        report.IndexFingerprints.Should().BeEmpty();
        report.ScenarioResults.Should().BeEmpty();
        report.ExplainResults.Should().BeEmpty();
        report.NoisyNeighbourAnalysis.Should().BeNull();
    }

    [Fact]
    public void WithExpression_PreservesImmutability()
    {
        var original = new BenchmarkReport
        {
            Status = "Completed",
            TotalElapsedSeconds = 10.5
        };

        var modified = original with { Status = "Cancelled" };

        original.Status.Should().Be("Completed");
        modified.Status.Should().Be("Cancelled");
        modified.TotalElapsedSeconds.Should().Be(10.5);
    }
}

public class DriverMetricsTests
{
    [Fact]
    public void DefaultValues_AreEmpty()
    {
        var metrics = new DriverMetrics();

        metrics.CommandLatency.Should().BeEmpty();
        metrics.CommandFailures.Should().BeEmpty();
        metrics.ConnectionCheckoutWait.Should().BeNull();
        metrics.CheckoutFailures.Should().Be(0);
        metrics.ConnectionsCreated.Should().Be(0);
        metrics.ConnectionsClosed.Should().Be(0);
        metrics.PoolClearedEvents.Should().Be(0);
    }
}

public class LatencyStatsTests
{
    [Fact]
    public void DefaultValues_AreZero()
    {
        var stats = new LatencyStats();

        stats.AvgMs.Should().Be(0);
        stats.P50Ms.Should().Be(0);
        stats.P95Ms.Should().Be(0);
        stats.P99Ms.Should().Be(0);
        stats.MinMs.Should().Be(0);
        stats.MaxMs.Should().Be(0);
    }
}

public class ScenarioResultTests
{
    [Fact]
    public void DefaultValues()
    {
        var result = new ScenarioResult();

        result.ScenarioName.Should().BeNull();
        result.TotalOperations.Should().Be(0);
        result.ErrorCount.Should().Be(0);
    }
}

public class RedFlagTests
{
    [Fact]
    public void CanConstruct_WithAllProperties()
    {
        var flag = new RedFlag
        {
            Category = "Test.Category",
            Severity = RiskLevel.Warning,
            Description = "desc",
            Remediation = "fix it",
            ObservedValue = 42,
            Threshold = 10
        };

        flag.Category.Should().Be("Test.Category");
        flag.Severity.Should().Be(RiskLevel.Warning);
        flag.ObservedValue.Should().Be(42);
        flag.Threshold.Should().Be(10);
    }
}

public class NoisyNeighbourAnalysisTests
{
    [Fact]
    public void Defaults_AreEmpty()
    {
        var analysis = new NoisyNeighbourAnalysis();

        analysis.Flags.Should().BeEmpty();
        analysis.ProbableCause.Should().BeNull();
        analysis.HasRedFlags.Should().BeFalse();
        analysis.OverallRisk.Should().Be(RiskLevel.None);
    }

    [Fact]
    public void HasRedFlags_WithFlags_ReturnsTrue()
    {
        var analysis = new NoisyNeighbourAnalysis
        {
            Flags = [new RedFlag { Category = "Test", Severity = RiskLevel.Warning }]
        };

        analysis.HasRedFlags.Should().BeTrue();
    }

    [Fact]
    public void OverallRisk_ReturnsHighestSeverity()
    {
        var analysis = new NoisyNeighbourAnalysis
        {
            Flags =
            [
                new RedFlag { Category = "A", Severity = RiskLevel.Warning },
                new RedFlag { Category = "B", Severity = RiskLevel.Critical }
            ]
        };

        analysis.OverallRisk.Should().Be(RiskLevel.Critical);
    }
}

public class DatasetFingerprintTests
{
    [Fact]
    public void DefaultValues()
    {
        var fp = new DatasetFingerprint();

        fp.CollectionName.Should().BeNull();
        fp.DocumentCount.Should().Be(0);
        fp.AvgDocumentSizeBytes.Should().Be(0);
        fp.P95DocumentSizeBytes.Should().Be(0);
    }
}

public class IndexFingerprintTests
{
    [Fact]
    public void DefaultValues()
    {
        var fp = new IndexFingerprint();

        fp.CollectionName.Should().BeNull();
        fp.IndexName.Should().BeNull();
        fp.IsUnique.Should().BeFalse();
    }
}
