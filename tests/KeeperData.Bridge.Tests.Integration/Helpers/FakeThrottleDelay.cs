using KeeperData.Core.Throttling;

namespace KeeperData.Bridge.Tests.Integration.Helpers;

public sealed class FakeThrottleDelay : IThrottleDelay
{
    public Task DelayAsync(int milliseconds, CancellationToken ct) => Task.CompletedTask;
}
