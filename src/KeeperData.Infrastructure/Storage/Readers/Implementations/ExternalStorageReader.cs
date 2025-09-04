using Amazon.S3;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Factories;
using System.Diagnostics.CodeAnalysis;

namespace KeeperData.Infrastructure.Storage.Readers.Implementations;

/// <summary>
/// Wiring up the IS3ClientFactory and ExternalStorageClient into an ExternalStorageReader.
/// Implementation to be completed in future tickets.
/// </summary>
/// <param name="s3ClientFactory">A single <see cref="IS3ClientFactory"/> reference.</param>
[ExcludeFromCodeCoverage]
public class ExternalStorageReader(IS3ClientFactory s3ClientFactory) : IStorageReader<ExternalStorageClient>
{
    private readonly IAmazonS3 _s3Client = s3ClientFactory.GetClient<ExternalStorageClient>();
    private readonly string _bucketName = s3ClientFactory.GetClientBucketName<ExternalStorageClient>();
}
