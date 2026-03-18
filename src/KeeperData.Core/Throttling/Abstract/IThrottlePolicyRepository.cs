using KeeperData.Core.Throttling.Models;

namespace KeeperData.Core.Throttling.Abstract;

public interface IThrottlePolicyRepository
{
    Task<IReadOnlyList<ThrottlePolicy>> GetAllAsync(CancellationToken ct = default);
    Task<ThrottlePolicy?> GetBySlugAsync(string slug, CancellationToken ct = default);
    Task<ThrottlePolicy?> GetActiveAsync(CancellationToken ct = default);
    Task UpsertAsync(ThrottlePolicy policy, CancellationToken ct = default);
    Task<bool> DeleteAsync(string slug, CancellationToken ct = default);
    Task DeactivateAllAsync(CancellationToken ct = default);
    Task<long> CountAsync(CancellationToken ct = default);
}
