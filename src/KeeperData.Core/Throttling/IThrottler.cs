using KeeperData.Core.Throttling.Abstract;
using KeeperData.Core.Throttling.Models;

namespace KeeperData.Core.Throttling;

/// <summary>
/// Combines policy settings access with a delay mechanism.
/// Reduces constructor parameter counts in consumers that need both.
/// </summary>
public interface IThrottler
{
    ThrottlePolicySettings Settings { get; }
    string ActivePolicyName { get; }
    string ActivePolicySlug { get; }
    Task DelayAsync(int milliseconds, CancellationToken ct);
}
