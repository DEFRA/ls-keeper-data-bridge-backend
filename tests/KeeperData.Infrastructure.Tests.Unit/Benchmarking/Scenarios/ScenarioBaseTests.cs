using FluentAssertions;
using KeeperData.Infrastructure.Benchmarking.Models;
using KeeperData.Infrastructure.Benchmarking.Scenarios;
using KeeperData.Infrastructure.Benchmarking.Throttling;

namespace KeeperData.Infrastructure.Tests.Unit.Benchmarking.Scenarios;

public class ScenarioBaseTests
{
    private sealed class SucceedingScenario : ScenarioBase
    {
        public override string Name => "Succeeding";
        protected override Task<bool> ExecuteOperationAsync(int iteration, CancellationToken ct)
            => Task.FromResult(true);
    }

    private sealed class FailingScenario : ScenarioBase
    {
        public override string Name => "Failing";
        protected override Task<bool> ExecuteOperationAsync(int iteration, CancellationToken ct)
            => Task.FromResult(false);
    }

    private sealed class ThrowingScenario : ScenarioBase
    {
        public override string Name => "Throwing";
        protected override Task<bool> ExecuteOperationAsync(int iteration, CancellationToken ct)
            => throw new InvalidOperationException("boom");
    }

    private sealed class CountingScenario : ScenarioBase
    {
        private int _callCount;
        public int CallCount => _callCount;

        public override string Name => "Counting";
        protected override Task<bool> ExecuteOperationAsync(int iteration, CancellationToken ct)
        {
            Interlocked.Increment(ref _callCount);
            return Task.FromResult(true);
        }
    }

    private static BenchmarkConfig ShortConfig(int concurrency = 1) => new()
    {
        Duration = TimeSpan.FromMilliseconds(200),
        Concurrency = concurrency,
        ThrottleDelay = TimeSpan.Zero
    };

    [Fact]
    public async Task RunAsync_SucceedingScenario_ReturnsZeroErrors()
    {
        var scenario = new SucceedingScenario();

        var result = await scenario.RunAsync(ShortConfig(), new NoOpBenchmarkThrottler(), CancellationToken.None);

        result.ScenarioName.Should().Be("Succeeding");
        result.TotalOperations.Should().BeGreaterThan(0);
        result.ErrorCount.Should().Be(0);
        result.ElapsedSeconds.Should().BeGreaterThan(0);
        result.OpsPerSecond.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task RunAsync_FailingScenario_CountsErrors()
    {
        var scenario = new FailingScenario();

        var result = await scenario.RunAsync(ShortConfig(), new NoOpBenchmarkThrottler(), CancellationToken.None);

        result.ScenarioName.Should().Be("Failing");
        result.ErrorCount.Should().Be(result.TotalOperations, "every operation returns false");
    }

    [Fact]
    public async Task RunAsync_ThrowingScenario_CountsErrorsGracefully()
    {
        var scenario = new ThrowingScenario();

        var result = await scenario.RunAsync(ShortConfig(), new NoOpBenchmarkThrottler(), CancellationToken.None);

        result.ErrorCount.Should().Be(result.TotalOperations, "every operation throws");
        result.TotalOperations.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task RunAsync_WithConcurrency_RunsMultipleWorkers()
    {
        var scenario = new CountingScenario();

        var result = await scenario.RunAsync(ShortConfig(concurrency: 4), new NoOpBenchmarkThrottler(), CancellationToken.None);

        result.TotalOperations.Should().BeGreaterThan(0);
        scenario.CallCount.Should().Be(result.TotalOperations);
    }

    [Fact]
    public async Task RunAsync_Cancellation_StopsEarly()
    {
        var scenario = new SucceedingScenario();
        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMilliseconds(50));

        var config = new BenchmarkConfig
        {
            Duration = TimeSpan.FromMinutes(5),
            Concurrency = 1,
            ThrottleDelay = TimeSpan.Zero
        };

        var result = await scenario.RunAsync(config, new NoOpBenchmarkThrottler(), cts.Token);

        result.ElapsedSeconds.Should().BeLessThan(5, "should stop well before the 5-minute duration");
    }

    [Fact]
    public async Task RunAsync_RecordsLatencyStats()
    {
        var scenario = new SucceedingScenario();

        var result = await scenario.RunAsync(ShortConfig(), new NoOpBenchmarkThrottler(), CancellationToken.None);

        result.Latency.Should().NotBeNull();
        result.Latency.MinMs.Should().BeGreaterThanOrEqualTo(0);
        result.Latency.MaxMs.Should().BeGreaterThanOrEqualTo(result.Latency.MinMs);
        result.Latency.AvgMs.Should().BeGreaterThanOrEqualTo(0);
    }

    [Fact]
    public async Task RunAsync_ComputesEffectiveOpsPerSecond()
    {
        var scenario = new SucceedingScenario();

        var result = await scenario.RunAsync(ShortConfig(), new NoOpBenchmarkThrottler(), CancellationToken.None);

        result.EffectiveOpsPerSecond.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task RunAsync_WithThrottler_StillCompletes()
    {
        var scenario = new SucceedingScenario();
        var config = new BenchmarkConfig
        {
            Duration = TimeSpan.FromMilliseconds(200),
            Concurrency = 1,
            ThrottleDelay = TimeSpan.FromMilliseconds(10)
        };

        var result = await scenario.RunAsync(config, new DefaultBenchmarkThrottler(), CancellationToken.None);

        result.TotalOperations.Should().BeGreaterThan(0);
        result.ErrorCount.Should().Be(0);
    }

    [Fact]
    public void Name_ReturnsScenarioName()
    {
        IBenchmarkScenario scenario = new SucceedingScenario();
        scenario.Name.Should().Be("Succeeding");
    }
}
