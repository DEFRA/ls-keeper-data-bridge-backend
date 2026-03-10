using KeeperData.SamAPI.Customers;

namespace KeeperData.SamAPI
{
    public interface ISamApi
    {
        Task<FindCustomersResponse?> FindCustomersAsync(IEnumerable<string> ids, CancellationToken ct = default);
    }
}