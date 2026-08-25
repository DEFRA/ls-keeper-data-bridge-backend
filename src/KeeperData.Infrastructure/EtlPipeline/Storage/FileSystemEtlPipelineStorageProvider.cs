using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage;
using KeeperData.Infrastructure.Storage;
using KeeperData.Infrastructure.Storage.Configuration;
using Microsoft.Extensions.Logging;

namespace KeeperData.Infrastructure.EtlPipeline.Storage;

/// <summary>Serves the ETL pipeline folders from the local file system, for local development
/// where <see cref="StorageConfiguration.UseFileSystem"/> is enabled.</summary>
public sealed class FileSystemEtlPipelineStorageProvider(
    ILoggerFactory loggerFactory,
    StorageConfiguration storageConfiguration) : IEtlPipelineStorageProvider
{
    private readonly string _basePath = FileSystemBlobStorageServiceFactory.ResolveBasePath(storageConfiguration);

    public IBlobStorageService ForFolder(string folder)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(folder);

        var logger = loggerFactory.CreateLogger<FileSystemBlobStorageService>();

        return new FileSystemBlobStorageService(logger, _basePath, folder);
    }
}
