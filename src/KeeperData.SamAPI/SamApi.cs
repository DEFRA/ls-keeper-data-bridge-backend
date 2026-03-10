using KeeperData.SamAPI.Customers;
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

        public async Task<FindCustomersResponse?> FindCustomersAsync(IEnumerable<string> ids, CancellationToken ct = default)
        {
            var token = await tokenClient.GetAccessTokenAsync(ct);

            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

            var response = await http.PostAsJsonAsync(
                "alpha/customers/find",
                new { ids },
                ct);

            response.EnsureSuccessStatusCode();

            return (await response.Content.ReadFromJsonAsync<FindCustomersResponse>(cancellationToken: ct));
        }
    }
}