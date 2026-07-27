using KeeperData.Core.Storage;

namespace KeeperData.Core.EtlPipeline.Storage;

/// <summary>Gives the ETL pipeline stages access to their own folders in the internal bucket.
///
/// This is deliberately separate from <see cref="IBlobStorageServiceFactory"/>: that factory is
/// bound to the legacy ETL folders and is left alone. The pipeline owns its folders through here.</summary>
public interface IEtlPipelineStorage
{
    /// <summary>Returns a storage service rooted at one of the <see cref="EtlPipelineFolders"/>.
    /// Object keys passed to the returned service are relative to that folder.</summary>
    IBlobStorageService ForFolder(string folder);
}
