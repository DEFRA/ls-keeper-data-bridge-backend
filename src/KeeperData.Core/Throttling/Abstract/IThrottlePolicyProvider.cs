using KeeperData.Core.Throttling.Models;

namespace KeeperData.Core.Throttling.Abstract;

public interface IThrottlePolicyProvider
{
    ThrottlePolicySettings Current { get; }
    string ActivePolicyName { get; }

    void Refresh(ThrottlePolicy? activePolicy);
}
