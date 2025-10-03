using KeeperData.Core.Storage;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Configuration;
using KeeperData.Infrastructure.Storage.Factories;
using Microsoft.Extensions.Logging;

namespace KeeperData.Infrastructure.Storage;

public static class BlobStorageSources
{
    public const string Internal = "internal";
    public const string External = "external";
}

public class S3BlobStorageServiceFactory(IS3ClientFactory s3ClientFactory, ILoggerFactory loggerFactory,
    StorageConfiguration storageConfiguration) : IBlobStorageServiceFactory
{

    public IBlobStorageServiceReadOnly GetSource(string type)
    {
        if (type == BlobStorageSources.External)
        {
            return GetSourceExternal();
        }
        else if (type == BlobStorageSources.Internal)
        {
            return GetSourceInternal();
        }
        else
        {
            throw new ArgumentException($"Value for parameter `{nameof(type)}` was expected to be " +
                $"'{BlobStorageSources.External}' or '{BlobStorageSources.Internal}', but was '{type}'");
        }
    }


    /// <summary>
    /// Gets the blob service for the external source S3 (IBM)
    /// </summary>
    /// <returns></returns>
    public IBlobStorageServiceReadOnly GetSourceExternal()
    {
        var info = s3ClientFactory.GetClientInfo<ExternalStorageClient>();
        var logger = loggerFactory.CreateLogger<S3BlobStorageServiceReadOnly>();
        return new S3BlobStorageServiceReadOnly(info.Client, logger, info.BucketName, storageConfiguration.SourceExternalPrefix);
    }

    /// <summary>
    /// Gets the blob service for the internal source S3 (CDP)
    /// </summary>
    /// <returns></returns>
    public IBlobStorageService GetSourceInternal()
    {
        var info = s3ClientFactory.GetClientInfo<InternalStorageClient>();
        var logger = loggerFactory.CreateLogger<S3BlobStorageService>();
        return new S3BlobStorageService(info.Client, logger, info.BucketName, storageConfiguration.SourceInternalPrefix);
    }

    /// <summary>
    /// Gets the main blob storage service 
    /// </summary>
    /// <returns></returns>
    public IBlobStorageService Get()
    {
        var info = s3ClientFactory.GetClientInfo<InternalStorageClient>();
        var logger = loggerFactory.CreateLogger<S3BlobStorageService>();
        return new S3BlobStorageService(info.Client, logger, info.BucketName, storageConfiguration.TargetInternalPrefix);
    }
}