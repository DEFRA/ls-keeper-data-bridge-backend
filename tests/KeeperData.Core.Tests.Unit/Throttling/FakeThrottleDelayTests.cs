using FluentAssertions;

namespace KeeperData.Core.Tests.Unit.Throttling;

public class FakeThrottleDelayTests
{
    [Fact]
    public async Task DelayAsync_ShouldRecordInvocations()
    {
        var sut = new FakeThrottleDelay();

        await sut.DelayAsync(100, CancellationToken.None);
        await sut.DelayAsync(200, CancellationToken.None);
        await sut.DelayAsync(0, CancellationToken.None);

        sut.TotalInvocations.Should().Be(3);
        sut.Invocations.Should().ContainInOrder(100, 200, 0);
    }

    [Fact]
    public async Task Reset_ShouldClearInvocations()
    {
        var sut = new FakeThrottleDelay();
        await sut.DelayAsync(50, CancellationToken.None);

        sut.Reset();

        sut.TotalInvocations.Should().Be(0);
    }
}
