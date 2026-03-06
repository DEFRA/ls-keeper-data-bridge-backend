using KeeperData.Core.Throttling.Abstract;
using KeeperData.Core.Throttling.Models;

namespace KeeperData.Core.Throttling.Impl;

public sealed class ThrottlePolicyProvider : IThrottlePolicyProvider
{
    private volatile ThrottlePolicySettings _current = ThrottlePolicyDefaults.NormalPolicy.Settings;
    private volatile string _activePolicyName = ThrottlePolicyDefaults.NormalName;
    private volatile string _activePolicySlug = ThrottlePolicyDefaults.NormalSlug;

    public ThrottlePolicySettings Current => _current;
    public string ActivePolicyName => _activePolicyName;
    public string ActivePolicySlug => _activePolicySlug;

    public void Refresh(ThrottlePolicy? activePolicy)
    {
        if (activePolicy is not null)
        {
            _current = activePolicy.Settings;
            _activePolicyName = activePolicy.Name;
            _activePolicySlug = activePolicy.Slug;
        }
        else
        {
            _current = ThrottlePolicyDefaults.NormalPolicy.Settings;
            _activePolicyName = ThrottlePolicyDefaults.NormalName;
            _activePolicySlug = ThrottlePolicyDefaults.NormalSlug;
        }
    }
}
