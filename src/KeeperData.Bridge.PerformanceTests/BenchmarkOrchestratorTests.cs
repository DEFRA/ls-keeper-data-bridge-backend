using FluentAssertions;
using KeeperData.Infrastructure.Benchmarking.Analysis;
using KeeperData.Infrastructure.Benchmarking.Models;
using KeeperData.Infrastructure.Benchmarking.Services;
using KeeperData.Infrastructure.Benchmarking.Throttling;
using KeeperData.Infrastructure.Json;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Testcontainers.MongoDb;
using Xunit.Abstractions;

namespace KeeperData.Bridge.PerformanceTests;

[Trait("testtype", "performance")]
public class BenchmarkOrchestratorTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private MongoDbContainer? _mongoDbContainer;
    private const string TestDatabaseName = "benchmark-test-db";

    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter(), new BsonDocumentJsonConverter() }
    };

    private static readonly string s_reportDirectory = Path.Combine(
        Path.GetDirectoryName(typeof(BenchmarkOrchestratorTests).Assembly.Location)!,
        "BenchmarkReports");

    public BenchmarkOrchestratorTests(ITestOutputHelper output)
    {
        _output = output;
    }

    public async Task InitializeAsync()
    {
        _output.WriteLine("=== Starting MongoDB container for benchmark tests ===");

        _mongoDbContainer = new MongoDbBuilder()
            .WithImage("mongo:7.0")
            .Build();

        await _mongoDbContainer.StartAsync();
        _output.WriteLine($"  MongoDB container started: {_mongoDbContainer.GetConnectionString()}");

        Directory.CreateDirectory(s_reportDirectory);
    }

    public async Task DisposeAsync()
    {
        if (_mongoDbContainer is not null)
        {
            await _mongoDbContainer.DisposeAsync();
            _output.WriteLine("  MongoDB container disposed");
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────

    private BenchmarkOrchestrator CreateOrchestrator()
    {
        var settings = MongoClientSettings.FromConnectionString(_mongoDbContainer!.GetConnectionString());
        settings.ApplicationName = TestDatabaseName;

        var loggerFactory = LoggerFactory.Create(b => b.AddConsole().SetMinimumLevel(LogLevel.Warning));
        var logger = loggerFactory.CreateLogger<BenchmarkOrchestrator>();

        return new BenchmarkOrchestrator(settings, new NoOpBenchmarkThrottler(), logger);
    }

    private async Task<string> WriteReportToDiskAsync(BenchmarkReport report, string label)
    {
        var json = JsonSerializer.Serialize(report, s_jsonOptions);
        var timestamp = report.TimestampUtc.ToString("yyyyMMdd_HHmmss");
        var fileName = $"benchmark_{label}_{timestamp}.json";
        var filePath = Path.Combine(s_reportDirectory, fileName);

        await File.WriteAllTextAsync(filePath, json);
        _output.WriteLine($"  Report written to: {filePath}");

        return filePath;
    }

    private void PrintReport(BenchmarkReport report)
    {
        var sb = new StringBuilder();
        sb.AppendLine();
        sb.AppendLine("╔══════════════════════════════════════════════════════════════╗");
        sb.AppendLine("║                    BENCHMARK REPORT                         ║");
        sb.AppendLine("╚══════════════════════════════════════════════════════════════╝");
        sb.AppendLine();
        sb.AppendLine($"  Environment:    {report.Environment}");
        sb.AppendLine($"  Timestamp:      {report.TimestampUtc:O}");
        sb.AppendLine($"  Status:         {report.Status}");
        sb.AppendLine($"  Total Elapsed:  {report.TotalElapsedSeconds}s");
        sb.AppendLine($"  Seed Count:     {report.Config.SeedCount}");
        sb.AppendLine($"  Concurrency:    {report.Config.Concurrency}");
        sb.AppendLine($"  Duration Cap:   {report.Config.Duration}");
        sb.AppendLine($"  Throttle Delay: {report.Config.ThrottleDelay.TotalMilliseconds}ms");
        sb.AppendLine();

        // ── Dataset fingerprints
        sb.AppendLine("── Dataset Fingerprints ───────────────────────────────────────");
        foreach (var fp in report.DatasetFingerprints)
        {
            sb.AppendLine($"  {fp.CollectionName}: {fp.DocumentCount} docs, avg={fp.AvgDocumentSizeBytes}B, p95={fp.P95DocumentSizeBytes}B");
        }

        sb.AppendLine();

        // ── Index fingerprints
        sb.AppendLine("── Index Fingerprints ─────────────────────────────────────────");
        foreach (var idx in report.IndexFingerprints)
        {
            sb.AppendLine($"  {idx.CollectionName}.{idx.IndexName}: {idx.KeyDefinition} (unique={idx.IsUnique})");
        }

        sb.AppendLine();

        // ── Scenario results
        sb.AppendLine("── Scenario Results ───────────────────────────────────────────");
        foreach (var s in report.ScenarioResults)
        {
            sb.AppendLine($"  [{s.ScenarioName}]");
            sb.AppendLine($"    Ops:               {s.TotalOperations}");
            sb.AppendLine($"    Errors:            {s.ErrorCount}");
            sb.AppendLine($"    Elapsed:           {s.ElapsedSeconds}s");
            sb.AppendLine($"    Ops/sec (wall):    {s.OpsPerSecond}");
            sb.AppendLine($"    Ops/sec (effective): {s.EffectiveOpsPerSecond}");
            sb.AppendLine($"    Avg:               {s.Latency.AvgMs}ms");
            sb.AppendLine($"    P50:               {s.Latency.P50Ms}ms");
            sb.AppendLine($"    P95:               {s.Latency.P95Ms}ms");
            sb.AppendLine($"    P99:               {s.Latency.P99Ms}ms");
            sb.AppendLine($"    Min:               {s.Latency.MinMs}ms");
            sb.AppendLine($"    Max:               {s.Latency.MaxMs}ms");
            sb.AppendLine();
        }

        // ── Driver metrics
        sb.AppendLine("── Driver Metrics ─────────────────────────────────────────────");
        var dm = report.DriverMetrics;

        sb.AppendLine("  Command Latencies:");
        foreach (var (cmd, lat) in dm.CommandLatency)
        {
            sb.AppendLine($"    {cmd,-16} avg={lat.AvgMs}ms  p50={lat.P50Ms}ms  p95={lat.P95Ms}ms  p99={lat.P99Ms}ms");
        }

        sb.AppendLine($"  Command Failures: {(dm.CommandFailures.Count == 0 ? "none" : string.Join(", ", dm.CommandFailures.Select(kv => $"{kv.Key}={kv.Value}")))}");

        if (dm.ConnectionCheckoutWait is not null)
        {
            sb.AppendLine($"  Checkout Wait:      avg={dm.ConnectionCheckoutWait.AvgMs}ms  p95={dm.ConnectionCheckoutWait.P95Ms}ms  p99={dm.ConnectionCheckoutWait.P99Ms}ms");
        }
        else
        {
            sb.AppendLine("  Checkout Wait:      (no data)");
        }

        sb.AppendLine($"  Checkout Failures:   {dm.CheckoutFailures}");
        sb.AppendLine($"  Connections Created: {dm.ConnectionsCreated}");
        sb.AppendLine($"  Connections Closed:  {dm.ConnectionsClosed}");
        sb.AppendLine($"  Pool Cleared Events: {dm.PoolClearedEvents}");
        sb.AppendLine();

        // ── Explain plans
        sb.AppendLine("── Explain Plans ──────────────────────────────────────────────");
        foreach (var ex in report.ExplainResults)
        {
            sb.AppendLine($"  [{ex.QueryName}]");
            sb.AppendLine($"    Winning Plan:      {Truncate(ex.WinningPlan, 120)}");
            sb.AppendLine($"    DocsExamined:      {ex.TotalDocsExamined}");
            sb.AppendLine($"    KeysExamined:      {ex.TotalKeysExamined}");
            sb.AppendLine($"    NReturned:         {ex.NReturned}");
            sb.AppendLine();
        }

        // ── Noisy neighbour analysis
        sb.AppendLine("── Noisy Neighbour Analysis ───────────────────────────────────");
        if (report.NoisyNeighbourAnalysis is { } analysis)
        {
            if (!analysis.HasRedFlags)
            {
                sb.AppendLine("  ✅ No red flags — environment appears healthy and uncontested.");
            }
            else
            {
                sb.AppendLine($"  ⚠️  {analysis.Flags.Count} red flag(s) detected (overall risk: {analysis.OverallRisk}):");
                foreach (var flag in analysis.Flags)
                {
                    sb.AppendLine($"    [{flag.Severity}] {flag.Category}");
                    sb.AppendLine($"      {flag.Description}");
                    sb.AppendLine($"      Observed: {flag.ObservedValue}  Threshold: {flag.Threshold}");
                    sb.AppendLine($"      → {flag.Remediation}");
                }

                if (analysis.ProbableCause is not null)
                {
                    sb.AppendLine();
                    sb.AppendLine($"  📋 Probable cause: {analysis.ProbableCause}");
                }
            }
        }

        sb.AppendLine();
        sb.AppendLine("═══════════════════════════════════════════════════════════════");

        _output.WriteLine(sb.ToString());
    }

    private static string Truncate(string value, int maxLength) =>
        value.Length <= maxLength ? value : string.Concat(value.AsSpan(0, maxLength), "…");

    // ── Tests ─────────────────────────────────────────────────────────

    /// <summary>
    /// Main benchmark: runs all 5 scenarios, writes JSON to disk, prints
    /// the full report, and asserts that no noisy-neighbour red flags fired.
    /// Configured to complete within ~1 minute against a local testcontainer.
    /// </summary>
    [Fact]
    public async Task RunAsync_CompletesSuccessfully_WithFullReport()
    {
        // Arrange
        var orchestrator = CreateOrchestrator();
        var config = new BenchmarkConfig
        {
            SeedCount = 1_000,
            Duration = TimeSpan.FromSeconds(8),
            ThrottleDelay = TimeSpan.FromMilliseconds(5),
            Concurrency = 2
        };

        // Act
        var report = await orchestrator.RunAsync(config, CancellationToken.None);

        // Assert – basic structure
        report.Should().NotBeNull();
        report!.Status.Should().Be("Completed");
        report.TotalElapsedSeconds.Should().BeGreaterThan(0);
        report.Config.Should().Be(config);

        // All 5 scenarios ran without errors
        report.ScenarioResults.Should().HaveCount(5);
        foreach (var s in report.ScenarioResults)
        {
            s.TotalOperations.Should().BeGreaterThan(0, $"scenario {s.ScenarioName} should have performed operations");
            s.ErrorCount.Should().Be(0, $"scenario {s.ScenarioName} should have no errors");
            s.OpsPerSecond.Should().BeGreaterThan(0);
            s.Latency.AvgMs.Should().BeGreaterThan(0);
        }

        // Driver metrics captured
        report.DriverMetrics.CommandLatency.Should().NotBeEmpty();
        report.DriverMetrics.CommandFailures.Should().BeEmpty();

        // Dataset fingerprints
        report.DatasetFingerprints.Should().HaveCount(3);
        var sourceFingerprint = report.DatasetFingerprints.First(f => f.CollectionName.Contains("source"));
        sourceFingerprint.DocumentCount.Should().Be(config.SeedCount);

        // Indexes
        report.IndexFingerprints.Should().Contain(i => i.IndexName == "ix_createdAt_status");
        report.IndexFingerprints.Should().Contain(i => i.IndexName == "ix_status_category");
        report.IndexFingerprints.Should().Contain(i => i.IndexName == "ix_referenceId");

        // Explain plans
        report.ExplainResults.Should().Contain(e => e.QueryName == "PointLookup");
        report.ExplainResults.Should().Contain(e => e.QueryName == "RangeQuery");
        report.ExplainResults.Should().Contain(e => e.QueryName == "Aggregation");

        // ── Noisy neighbour: a local testcontainer should be clean ──
        report.NoisyNeighbourAnalysis.Should().NotBeNull();
        report.NoisyNeighbourAnalysis!.HasRedFlags.Should().BeFalse(
            "a local testcontainer should show zero noisy-neighbour red flags — " +
            "if this fails, the thresholds may need recalibrating. " +
            $"Flags: {string.Join("; ", report.NoisyNeighbourAnalysis.Flags.Select(f => f.Description))}");

        // Orchestrator state
        orchestrator.LastReport.Should().BeSameAs(report);
        orchestrator.IsRunning.Should().BeFalse();

        // ── Write to disk & print ───────────────────────────────────
        PrintReport(report);
        var filePath = await WriteReportToDiskAsync(report, "full");

        File.Exists(filePath).Should().BeTrue();
        var onDisk = await File.ReadAllTextAsync(filePath);
        onDisk.Should().Contain("scenarioResults");
        onDisk.Should().Contain("noisyNeighbourAnalysis");
    }

    [Fact]
    public async Task RunAsync_CanBeCancelled_ProducesPartialReport()
    {
        var orchestrator = CreateOrchestrator();
        var config = new BenchmarkConfig
        {
            SeedCount = 500,
            Duration = TimeSpan.FromMinutes(10),
            ThrottleDelay = TimeSpan.FromMilliseconds(5)
        };

        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromSeconds(8));

        var report = await orchestrator.RunAsync(config, cts.Token);

        report.Should().NotBeNull();
        report!.Status.Should().Be("Cancelled");
        report.DriverMetrics.Should().NotBeNull();
        orchestrator.IsRunning.Should().BeFalse();

        // Verify cleanup
        var client = new MongoClient(_mongoDbContainer!.GetConnectionString());
        var db = client.GetDatabase(TestDatabaseName);
        using var cursor = await db.ListCollectionNamesAsync();
        var remaining = await cursor.ToListAsync();
        remaining.Should().NotContain(c => c.StartsWith(config.CollectionPrefix));

        await WriteReportToDiskAsync(report, "cancelled");
    }

    [Fact]
    public async Task RunAsync_RejectsSecondConcurrentRun()
    {
        var orchestrator = CreateOrchestrator();
        var config = new BenchmarkConfig
        {
            SeedCount = 200,
            Duration = TimeSpan.FromMinutes(5),
            ThrottleDelay = TimeSpan.FromMilliseconds(5)
        };

        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromSeconds(8));

        var firstRunTask = orchestrator.RunAsync(config, cts.Token);
        await Task.Delay(500);

        orchestrator.IsRunning.Should().BeTrue();
        var secondResult = await orchestrator.RunAsync(config, CancellationToken.None);

        cts.Cancel();
        await firstRunTask;

        secondResult.Should().BeNull();
        orchestrator.IsRunning.Should().BeFalse();
    }

    [Fact]
    public async Task RunAsync_CleansUpCollections_AfterCompletion()
    {
        var orchestrator = CreateOrchestrator();
        var config = new BenchmarkConfig
        {
            SeedCount = 200,
            Duration = TimeSpan.FromSeconds(3),
            ThrottleDelay = TimeSpan.FromMilliseconds(5)
        };

        await orchestrator.RunAsync(config, CancellationToken.None);

        var client = new MongoClient(_mongoDbContainer!.GetConnectionString());
        var db = client.GetDatabase(TestDatabaseName);
        using var cursor = await db.ListCollectionNamesAsync();
        var remaining = await cursor.ToListAsync();

        remaining.Should().NotContain(c => c.StartsWith(config.CollectionPrefix));
    }

    [Fact]
    public async Task RunAsync_ExplainPlans_UseIndexes()
    {
        var orchestrator = CreateOrchestrator();
        var config = new BenchmarkConfig
        {
            SeedCount = 500,
            Duration = TimeSpan.FromSeconds(5),
            ThrottleDelay = TimeSpan.FromMilliseconds(5)
        };

        var report = await orchestrator.RunAsync(config, CancellationToken.None);

        report.Should().NotBeNull();

        var pointLookup = report!.ExplainResults.First(e => e.QueryName == "PointLookup");
        pointLookup.NReturned.Should().Be(1);
        pointLookup.TotalDocsExamined.Should().BeLessOrEqualTo(1);

        var rangeQuery = report.ExplainResults.First(e => e.QueryName == "RangeQuery");
        rangeQuery.TotalKeysExamined.Should().BeGreaterThan(0);
        rangeQuery.WinningPlan.Should().Contain("IXSCAN");
    }

    [Fact]
    public async Task RunAsync_DriverMetrics_CapturesConnectionPoolEvents()
    {
        var orchestrator = CreateOrchestrator();
        var config = new BenchmarkConfig
        {
            SeedCount = 500,
            Duration = TimeSpan.FromSeconds(5),
            ThrottleDelay = TimeSpan.FromMilliseconds(5)
        };

        var report = await orchestrator.RunAsync(config, CancellationToken.None);

        report.Should().NotBeNull();
        var dm = report!.DriverMetrics;

        dm.CommandLatency.Should().ContainKey("find");
        dm.CommandLatency.Should().ContainKey("update");
        dm.ConnectionsCreated.Should().BeGreaterThan(0);
        dm.CheckoutFailures.Should().Be(0);
        dm.PoolClearedEvents.Should().Be(0);
    }

    [Fact]
    public async Task RunAsync_ProducesSerializableJsonReport()
    {
        var orchestrator = CreateOrchestrator();
        var config = new BenchmarkConfig
        {
            SeedCount = 200,
            Duration = TimeSpan.FromSeconds(3),
            ThrottleDelay = TimeSpan.FromMilliseconds(5)
        };

        var report = await orchestrator.RunAsync(config, CancellationToken.None);
        report.Should().NotBeNull();

        var json = JsonSerializer.Serialize(report, s_jsonOptions);
        json.Should().NotBeNullOrWhiteSpace();

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        root.TryGetProperty("environment", out _).Should().BeTrue();
        root.TryGetProperty("scenarioResults", out _).Should().BeTrue();
        root.TryGetProperty("driverMetrics", out _).Should().BeTrue();
        root.TryGetProperty("noisyNeighbourAnalysis", out _).Should().BeTrue();
    }
}
