using KeeperData.Core.Throttling;
using KeeperData.Core.Throttling.Models;

namespace KeeperData.Bridge.Tests.Integration.Helpers;

public sealed class FakeThrottler : IThrottler
{
    public ThrottlePolicySettings Settings { get; set; } = new TestThrottlePolicyProvider().Current;
    public string ActivePolicyName { get; set; } = "UnitTest";
    public string ActivePolicySlug { get; set; } = "unit-test";

    public Task DelayAsync(int milliseconds, CancellationToken ct) => Task.CompletedTask;
}
