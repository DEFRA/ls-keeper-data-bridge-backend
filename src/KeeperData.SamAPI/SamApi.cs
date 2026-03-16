using KeeperData.SamAPI.Customers;
using KeeperData.SamAPI.Holdings;
using KeeperData.SamAPI.Locations;
using KeeperData.SamAPI.Security;
using System.Net.Http.Headers;
using System.Net.Http.Json;

namespace KeeperData.SamAPI
{
    public sealed class SamApi : ISamApi
    {
        private readonly HttpClient http;
        private readonly ITokenClient tokenClient;

        public SamApi(HttpClient http, ITokenClient tokenClient)
        {
            this.http = http;
            this.tokenClient = tokenClient;
        }

        public async Task<FindCustomersResponse?> FindCustomers(
            IEnumerable<string> ids,
            int page = 1,
            int pageSize = 50,
            CancellationToken ct = default)
        {
            var token = await tokenClient.GetAccessTokenAsync(ct);

            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

            var response = await http.PostAsJsonAsync(
                $"customers/find?page={page}&pageSize={pageSize}",
                new { ids },
                ct);

            response.EnsureSuccessStatusCode();

            return (await response.Content.ReadFromJsonAsync<FindCustomersResponse>(cancellationToken: ct));
        }

        public async Task<FindHoldingsResponse?> FindHoldings(
            IEnumerable<string> ids,
            int page = 1,
            int pageSize = 50,
            CancellationToken ct = default)
        {
            var token = await tokenClient.GetAccessTokenAsync(ct);

            http.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            http.DefaultRequestHeaders.Accept.Clear();
            http.DefaultRequestHeaders.Accept.Add(
                new MediaTypeWithQualityHeaderValue("application/vnd.apha.1+json"));

            var response = await http.PostAsJsonAsync(
                $"alpha/holdings/find?page={page}&pageSize={pageSize}",
                new { ids },
                ct);

            response.EnsureSuccessStatusCode();

            return await response.Content.ReadFromJsonAsync<FindHoldingsResponse>(cancellationToken: ct);
        }

        public async Task<GetHoldingResponse?> GetHoldings(
           string countyId,
           string parishId,
           string holdingId,
           CancellationToken ct = default)
        {
            var token = await tokenClient.GetAccessTokenAsync(ct);

            http.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            http.DefaultRequestHeaders.Accept.Clear();
            http.DefaultRequestHeaders.Accept.Add(
                new MediaTypeWithQualityHeaderValue("application/vnd.apha.1+json"));

            var response = await http.GetAsync(
                $"holdings/{countyId}/{parishId}/{holdingId}",
                ct);

            response.EnsureSuccessStatusCode();

            return await response.Content.ReadFromJsonAsync<GetHoldingResponse>(cancellationToken: ct);
        }

        public async Task<FindLocationsResponse?> FindLocations(
            IEnumerable<string> ids,
            int page = 1,
            int pageSize = 50,
            CancellationToken ct = default)
        {
            var token = await tokenClient.GetAccessTokenAsync(ct);

            http.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            http.DefaultRequestHeaders.Accept.Clear();
            http.DefaultRequestHeaders.Accept.Add(
                new MediaTypeWithQualityHeaderValue("application/vnd.apha.1+json"));

            var response = await http.PostAsJsonAsync(
                $"locations/find?page={page}&pageSize={pageSize}",
                new { ids },
                ct);

            response.EnsureSuccessStatusCode();

            return await response.Content.ReadFromJsonAsync<FindLocationsResponse>(cancellationToken: ct);
        }
    }
}