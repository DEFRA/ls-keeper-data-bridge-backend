using Amazon.S3;

namespace KeeperData.Infrastructure.Storage.Factories;

public interface IS3ClientFactory
{
    IAmazonS3 GetClient<T>()
        where T : IStorageClient, new();

    IAmazonS3 GetClient(string clientName);

    string GetClientBucketName<T>()
        where T : IStorageClient, new();

    string GetClientBucketName(string clientName);

    IEnumerable<string> GetRegisteredClientNames();

    bool HasStorageClient(string storageClientName);

    void AddClient<T>(string defaultBucketName, AmazonS3Config amazonS3Config)
        where T : IStorageClient, new();

    void AddClientWithCredentials<T>(string defaultBucketName, string accessKeyRef, string secretKeyRef, AmazonS3Config amazonS3Config)
        where T : IStorageClient, new();

    void RegisterMockClient<T>(string bucketName, IAmazonS3 mockClient)
        where T : IStorageClient, new();
}