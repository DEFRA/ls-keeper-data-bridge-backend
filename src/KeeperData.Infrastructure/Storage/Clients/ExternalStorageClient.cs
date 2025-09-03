namespace KeeperData.Infrastructure.Storage.Clients;

public class ExternalStorageClient : IStorageClient
{
    public string ClientName => GetType().Name;
}
