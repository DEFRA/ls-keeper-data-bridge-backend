using KeeperData.Core.Throttling.Models;

namespace KeeperData.Core.Throttling.Abstract;

public interface IThrottlePolicyQueryService
{
    Task<IReadOnlyList<ThrottlePolicy>> GetAllAsync(CancellationToken ct = default);
    Task<ThrottlePolicy?> GetBySlugAsync(string slug, CancellationToken ct = default);
    ThrottlePolicy GetActive();
}
