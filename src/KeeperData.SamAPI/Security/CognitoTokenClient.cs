using Microsoft.Extensions.Options;
using System.Net.Http.Json;

namespace KeeperData.SamAPI.Security
{
    public sealed class CognitoTokenClient : ITokenClient
    {
        private readonly HttpClient http;
        private readonly SamApiOptions options;

        public CognitoTokenClient(
            HttpClient http,
            IOptions<SamApiOptions> options)
        {
            this.http = http;
            this.options = options.Value;
        }

        public async Task<string> GetAccessTokenAsync(CancellationToken ct = default)
        {
            var content = new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["grant_type"] = "client_credentials",
                ["client_id"] = options.ClientId,
                ["client_secret"] = options.ClientSecret
            });

            var response = await http.PostAsync("/oauth2/token", content, ct);
            response.EnsureSuccessStatusCode();

            var json = await response.Content.ReadFromJsonAsync<TokenResponse>(cancellationToken: ct);

            return json!.AccessToken;
        }
    }
}