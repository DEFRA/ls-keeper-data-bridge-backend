namespace KeeperData.Bridge.Tests.Integration;

public class AppTestFixture : IDisposable
{
    public readonly HttpClient HttpClient;
    public readonly AppWebApplicationFactory AppWebApplicationFactory;

    public AppTestFixture()
    {
        AppWebApplicationFactory = new AppWebApplicationFactory();
        HttpClient = AppWebApplicationFactory.CreateClient();
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
            AppWebApplicationFactory?.Dispose();
        }
    }
}