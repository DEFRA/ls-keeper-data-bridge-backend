using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Throttling.Abstract;
using KeeperData.Core.Throttling.Models;

namespace KeeperData.Core.Throttling.Impl;

[ExcludeFromCodeCoverage]
public sealed class Throttler(IThrottlePolicyProvider provider, IThrottleDelay delay) : IThrottler
{
    public ThrottlePolicySettings Settings => provider.Current;
    public string ActivePolicyName => provider.ActivePolicyName;
    public string ActivePolicySlug => provider.ActivePolicySlug;

    public Task DelayAsync(int milliseconds, CancellationToken ct)
        => delay.DelayAsync(milliseconds, ct);
}
