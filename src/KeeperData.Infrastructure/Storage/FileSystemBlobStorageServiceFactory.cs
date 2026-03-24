using KeeperData.Core.Storage;
using KeeperData.Infrastructure.Storage.Clients;
using KeeperData.Infrastructure.Storage.Configuration;
using KeeperData.Infrastructure.Storage.Factories;
using Microsoft.Extensions.Logging;

namespace KeeperData.Infrastructure.Storage;

/// <summary>
/// Factory that returns file-system-backed blob services for internal storage
/// while always delegating external source access to S3.
/// </summary>
public class FileSystemBlobStorageServiceFactory(
    IS3ClientFactory s3ClientFactory,
    ILoggerFactory loggerFactory,
    StorageConfiguration storageConfiguration) : IBlobStorageServiceFactory
{
    private readonly string _basePath = ResolveBasePath(storageConfiguration);

    public IBlobStorageServiceReadOnly GetSource(string type)
    {
        if (type == BlobStorageSources.External)
            return GetSourceExternal();

        if (type == BlobStorageSources.Internal)
            return GetSourceInternal();

        throw new ArgumentException(
            $"Value for parameter `{nameof(type)}` was expected to be " +
            $"'{BlobStorageSources.External}' or '{BlobStorageSources.Internal}', but was '{type}'");
    }

    public IBlobStorageServiceReadOnly GetSourceExternal()
    {
        var info = s3ClientFactory.GetClientInfo<ExternalStorageClient>();
        var logger = loggerFactory.CreateLogger<S3BlobStorageServiceReadOnly>();
        return new S3BlobStorageServiceReadOnly(info.Client, logger, info.BucketName, storageConfiguration.SourceExternalPrefix);
    }

    public IBlobStorageService GetSourceInternal()
    {
        var logger = loggerFactory.CreateLogger<FileSystemBlobStorageService>();
        return new FileSystemBlobStorageService(logger, _basePath, storageConfiguration.SourceInternalPrefix);
    }

    public IBlobStorageService Get()
    {
        var logger = loggerFactory.CreateLogger<FileSystemBlobStorageService>();
        return new FileSystemBlobStorageService(logger, _basePath, storageConfiguration.TargetInternalPrefix);
    }

    public IBlobStorageService GetCleanseReportsBlobService()
    {
        var logger = loggerFactory.CreateLogger<FileSystemBlobStorageService>();
        return new FileSystemBlobStorageService(logger, _basePath, "cleanse-reports");
    }

    internal static string ResolveBasePath(StorageConfiguration config)
    {
        if (!string.IsNullOrWhiteSpace(config.FileSystemBasePath))
            return config.FileSystemBasePath;

        return Path.Combine(Path.GetTempPath(), "keeper-data-bridge");
    }
}
