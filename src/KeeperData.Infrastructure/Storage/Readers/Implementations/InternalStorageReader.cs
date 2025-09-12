using Amazon.S3;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Factories;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.Storage.Readers.Implementations;

/// <summary>
/// Wiring up the IS3ClientFactory and InternalStorageClient into an InternalStorageReader.
/// </summary>
/// <param name="s3ClientFactory">A single <see cref="IS3ClientFactory"/> reference.</param>
[ExcludeFromCodeCoverage]
public class InternalStorageReader(IS3ClientFactory s3ClientFactory) : IStorageReader<InternalStorageClient>
{
    private readonly IAmazonS3 _s3Client = s3ClientFactory.GetClient<InternalStorageClient>();
    private readonly string _bucketName = s3ClientFactory.GetClientBucketName<InternalStorageClient>();
}