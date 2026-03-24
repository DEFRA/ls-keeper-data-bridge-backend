using FluentAssertions;
using KeeperData.Infrastructure.Benchmarking.Throttling;
using System.Diagnostics;

namespace KeeperData.Infrastructure.Tests.Unit.Benchmarking.Throttling;

public class BenchmarkThrottlerTests
{
    [Fact]
    public async Task DefaultBenchmarkThrottler_DelaysForConfiguredDuration()
    {
        var throttler = new DefaultBenchmarkThrottler();
        var delay = TimeSpan.FromMilliseconds(100);

        var sw = Stopwatch.StartNew();
        await throttler.DelayAsync(delay, CancellationToken.None);
        sw.Stop();

        sw.Elapsed.Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(80),
            "the throttler should wait approximately the configured delay");
    }

    [Fact]
    public async Task DefaultBenchmarkThrottler_ZeroDelay_ReturnsImmediately()
    {
        var throttler = new DefaultBenchmarkThrottler();

        var sw = Stopwatch.StartNew();
        await throttler.DelayAsync(TimeSpan.Zero, CancellationToken.None);
        sw.Stop();

        sw.Elapsed.Should().BeLessThan(TimeSpan.FromMilliseconds(50));
    }

    [Fact]
    public async Task DefaultBenchmarkThrottler_HonoursCancellation()
    {
        var throttler = new DefaultBenchmarkThrottler();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var act = () => throttler.DelayAsync(TimeSpan.FromSeconds(10), cts.Token);

        await act.Should().ThrowAsync<TaskCanceledException>();
    }

    [Fact]
    public async Task NoOpBenchmarkThrottler_ReturnsImmediately()
    {
        var throttler = new NoOpBenchmarkThrottler();

        var sw = Stopwatch.StartNew();
        await throttler.DelayAsync(TimeSpan.FromSeconds(10), CancellationToken.None);
        sw.Stop();

        sw.Elapsed.Should().BeLessThan(TimeSpan.FromMilliseconds(50),
            "the no-op throttler should never delay regardless of the configured value");
    }

    [Fact]
    public async Task NoOpBenchmarkThrottler_IgnoresCancellationToken()
    {
        var throttler = new NoOpBenchmarkThrottler();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Should not throw — it returns Task.CompletedTask without checking the token
        var act = () => throttler.DelayAsync(TimeSpan.FromSeconds(10), cts.Token);
        await act.Should().NotThrowAsync();
    }
}
