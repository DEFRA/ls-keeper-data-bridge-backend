using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage;
using KeeperData.Infrastructure.Storage;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Factories;
using Microsoft.Extensions.Logging;

namespace KeeperData.Infrastructure.EtlPipeline.Storage;

/// <summary>Serves the ETL pipeline folders from the internal S3 bucket. Each folder is a
/// top-level folder at the bucket root, alongside the legacy ETL folders.</summary>
public sealed class S3EtlPipelineStorageProvider(
    IS3ClientFactory s3ClientFactory,
    ILoggerFactory loggerFactory) : IEtlPipelineStorageProvider
{
    public IBlobStorageService ForFolder(string folder)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(folder);

        var info = s3ClientFactory.GetClientInfo<InternalStorageClient>();
        var logger = loggerFactory.CreateLogger<S3BlobStorageService>();

        return new S3BlobStorageService(info.Client, logger, info.BucketName, folder);
    }
}
