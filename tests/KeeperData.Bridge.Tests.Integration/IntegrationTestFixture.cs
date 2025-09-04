namespace KeeperData.Bridge.Tests.Integration;

public class IntegrationTestFixture : IDisposable
{
    public HttpClient HttpClient { get; }

    private readonly HttpClientHandler _httpClientHandler;

    public IntegrationTestFixture()
    {
        _httpClientHandler = new HttpClientHandler
        {
            AllowAutoRedirect = false
        };

        HttpClient = new HttpClient(_httpClientHandler)
        {
            BaseAddress = new Uri("http://localhost:8080"),
            Timeout = TimeSpan.FromSeconds(30)
        };
        HttpClient.DefaultRequestHeaders.Add("Accept", "application/json");
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            HttpClient?.Dispose();
            _httpClientHandler?.Dispose();
        }
    }
}