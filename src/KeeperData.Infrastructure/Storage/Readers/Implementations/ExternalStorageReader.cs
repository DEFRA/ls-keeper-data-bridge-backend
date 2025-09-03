using Amazon.S3;
using Amazon.S3.Model;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Factories;

namespace KeeperData.Infrastructure.Storage.Readers.Implementations;

public class ExternalStorageReader(IS3ClientFactory s3ClientFactory) : IStorageReader<ExternalStorageClient>
{
    private readonly IAmazonS3 _s3Client = s3ClientFactory.GetClient<ExternalStorageClient>();
    private readonly string _bucketName = s3ClientFactory.GetClientBucketName<ExternalStorageClient>();

    public async Task<string> ReadObjectAsStringAsync(string key)
    {
        using var response = await _s3Client.GetObjectAsync(_bucketName, key);
        using var reader = new StreamReader(response.ResponseStream);
        return await reader.ReadToEndAsync();
    }

    public async Task<IEnumerable<string>> ListObjectKeysAsync(string prefix)
    {
        var keys = new List<string>();
        string? continuationToken = null;

        do
        {
            var request = new ListObjectsV2Request
            {
                BucketName = _bucketName,
                Prefix = prefix,
                ContinuationToken = continuationToken
            };

            var response = await _s3Client.ListObjectsV2Async(request);
            keys.AddRange(response.S3Objects.Select(o => o.Key));
            continuationToken = (response?.IsTruncated ?? false) ? response.NextContinuationToken : null;

        } while (continuationToken != null);

        return keys;
    }
}
