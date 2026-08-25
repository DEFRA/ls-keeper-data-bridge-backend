using KeeperData.Core.Storage;

namespace KeeperData.Core.EtlPipeline.Storage;

/// <summary>Gives the ETL pipeline stages access to their own folders in the internal bucket.
public interface IEtlPipelineStorageProvider
{
    /// <summary>Returns a storage service rooted at one of the <see cref="EtlPipelineFolders"/>.
    IBlobStorageService ForFolder(string folder);
}
