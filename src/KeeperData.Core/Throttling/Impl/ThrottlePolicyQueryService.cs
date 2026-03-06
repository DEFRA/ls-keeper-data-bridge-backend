using KeeperData.Core.Throttling.Abstract;
using KeeperData.Core.Throttling.Models;

namespace KeeperData.Core.Throttling.Impl;

public sealed class ThrottlePolicyQueryService(
    IThrottlePolicyRepository repository,
    IThrottlePolicyProvider provider) : IThrottlePolicyQueryService
{
    public async Task<IReadOnlyList<ThrottlePolicy>> GetAllAsync(CancellationToken ct = default)
    {
        var stored = await repository.GetAllAsync(ct);
        var result = new List<ThrottlePolicy>(stored.Count + 1) { ThrottlePolicyDefaults.NormalPolicy };
        result.AddRange(stored);
        return result;
    }

    public async Task<ThrottlePolicy?> GetBySlugAsync(string slug, CancellationToken ct = default)
    {
        if (string.Equals(slug, ThrottlePolicyDefaults.NormalSlug, StringComparison.OrdinalIgnoreCase))
            return ThrottlePolicyDefaults.NormalPolicy;

        return await repository.GetBySlugAsync(slug, ct);
    }

    public ThrottlePolicy GetActive()
    {
        return new ThrottlePolicy
        {
            Slug = ThrottlePolicyDefaults.ToSlug(provider.ActivePolicyName),
            Name = provider.ActivePolicyName,
            IsActive = true,
            Settings = provider.Current
        };
    }
}
