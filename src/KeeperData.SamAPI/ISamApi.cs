using KeeperData.SamAPI.Customers;
using KeeperData.SamAPI.Holdings;
using KeeperData.SamAPI.Locations;

namespace KeeperData.SamAPI
{
    public interface ISamApi
    {
        Task<FindCustomersResponse?> FindCustomers(
            IEnumerable<string> ids,
            int page = 1,
            int pageSize = 50,
            CancellationToken ct = default);

        Task<GetHoldingResponse?> GetHoldings(
           string countyId,
           string parishId,
           string holdingId,
           CancellationToken ct = default);

        Task<FindHoldingsResponse?> FindHoldings(
            IEnumerable<string> ids,
            int page = 1,
            int pageSize = 50,
            CancellationToken ct = default);

        Task<FindLocationsResponse?> FindLocations(
           IEnumerable<string> ids,
           int page = 1,
           int pageSize = 50,
           CancellationToken ct = default);
    }
}