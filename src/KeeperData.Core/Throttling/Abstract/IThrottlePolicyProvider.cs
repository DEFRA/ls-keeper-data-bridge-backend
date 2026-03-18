using KeeperData.Core.Throttling.Models;

namespace KeeperData.Core.Throttling.Abstract;

public interface IThrottlePolicyProvider
{
    ThrottlePolicySettings Current { get; }
    string ActivePolicyName { get; }
    string ActivePolicySlug { get; }

    void Refresh(ThrottlePolicy? activePolicy);
}
