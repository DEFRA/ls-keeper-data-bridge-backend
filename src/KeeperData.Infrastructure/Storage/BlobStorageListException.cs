namespace KeeperData.Infrastructure.Storage;

/// <summary>Thrown when listing objects in a bucket fails. Wraps the underlying provider
/// exception (e.g. AmazonS3Exception, whose own message omits the bucket) and names the
/// bucket and prefix that were being listed, so the failure is diagnosable from the message
/// alone.</summary>
public sealed class BlobStorageListException(string bucketName, string? prefix, Exception innerException)
    : Exception($"Failed to list objects in bucket '{bucketName}' with prefix '{prefix ?? "(none)"}'. {innerException.Message}", innerException)
{
    public string BucketName { get; } = bucketName;

    public string? Prefix { get; } = prefix;
}
