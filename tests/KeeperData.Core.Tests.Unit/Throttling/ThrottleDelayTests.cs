using FluentAssertions;
using KeeperData.Core.Throttling;

namespace KeeperData.Core.Tests.Unit.Throttling;

public class ThrottleDelayTests
{
    [Fact]
    public async Task DelayAsync_WithPositiveMs_ShouldDelay()
    {
        var sut = new ThrottleDelay();
        var sw = System.Diagnostics.Stopwatch.StartNew();

        await sut.DelayAsync(50, CancellationToken.None);

        sw.Stop();
        sw.ElapsedMilliseconds.Should().BeGreaterThanOrEqualTo(30);
    }

    [Fact]
    public async Task DelayAsync_WithZero_ShouldNotDelay()
    {
        var sut = new ThrottleDelay();
        var sw = System.Diagnostics.Stopwatch.StartNew();

        await sut.DelayAsync(0, CancellationToken.None);

        sw.Stop();
        sw.ElapsedMilliseconds.Should().BeLessThan(50);
    }
}
