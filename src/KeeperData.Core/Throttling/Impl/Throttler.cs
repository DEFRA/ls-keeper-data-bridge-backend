using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.Throttling.Abstract;
using KeeperData.Core.Throttling.Models;

namespace KeeperData.Core.Throttling.Impl;

[ExcludeFromCodeCoverage]
public sealed class Throttler(IThrottlePolicyProvider provider, IThrottleDelay delay) : IThrottler
{
    public ThrottlePolicySettings Settings => provider.Current;

    public Task DelayAsync(int milliseconds, CancellationToken ct)
        => delay.DelayAsync(milliseconds, ct);
}
