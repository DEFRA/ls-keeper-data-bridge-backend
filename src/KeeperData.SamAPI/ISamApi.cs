using KeeperData.SamAPI.Customers;
using KeeperData.SamAPI.Holdings;

namespace KeeperData.SamAPI
{
    public interface ISamApi
    {
        Task<FindCustomersResponse?> FindCustomersAsync(
            IEnumerable<string> ids, 
            CancellationToken ct = default);

        Task<GetHoldingResponse?> GetHoldingAsync(
           string countyId,
           string parishId,
           string holdingId,
           CancellationToken ct = default);

        Task<FindHoldingsResponse?> FindHoldingsAsync(
            IEnumerable<string> ids,
            int page = 1,
            int pageSize = 50,
            CancellationToken ct = default);
    }
}