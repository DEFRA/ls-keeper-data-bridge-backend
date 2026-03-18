using KeeperData.Core.Throttling;
using KeeperData.Core.Throttling.Models;

namespace KeeperData.Core.Tests.Unit.Throttling;

public sealed class FakeThrottler : IThrottler
{
    public ThrottlePolicySettings Settings { get; set; } = TestThrottlePolicyProvider.UnitTestSettings;
    public string ActivePolicyName { get; set; } = "UnitTest";
    public string ActivePolicySlug { get; set; } = "unit-test";

    public Task DelayAsync(int milliseconds, CancellationToken ct) => Task.CompletedTask;
}
