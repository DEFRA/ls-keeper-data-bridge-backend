namespace KeeperData.Infrastructure.Storage.Readers;

public interface IStorageReader<T> where T : IStorageClient, new()
{
    Task<string> ReadObjectAsStringAsync(string key);
    Task<IEnumerable<string>> ListObjectKeysAsync(string prefix);
}
