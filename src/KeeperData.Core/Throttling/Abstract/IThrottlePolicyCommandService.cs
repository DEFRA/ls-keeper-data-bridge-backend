using KeeperData.Core.Throttling.Commands;
using KeeperData.Core.Throttling.Models;

namespace KeeperData.Core.Throttling.Abstract;

public interface IThrottlePolicyCommandService
{
    Task<ThrottlePolicy> CreateAsync(CreateThrottlePolicyCommand command, CancellationToken ct = default);
    Task<ThrottlePolicy> UpdateAsync(string slug, UpdateThrottlePolicyCommand command, CancellationToken ct = default);
    Task DeleteAsync(string slug, CancellationToken ct = default);
    Task<ThrottlePolicy> ActivateAsync(string slug, CancellationToken ct = default);
    Task DeactivateAllAsync(CancellationToken ct = default);
}
