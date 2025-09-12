namespace KeeperData.Infrastructure.Storage.Clients;

public class InternalStorageClient : IStorageClient
{
    public string ClientName => GetType().Name;
}