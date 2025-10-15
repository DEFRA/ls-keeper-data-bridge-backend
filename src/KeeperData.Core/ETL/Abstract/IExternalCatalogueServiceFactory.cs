using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Storage;

namespace KeeperData.Core.ETL.Abstract;

public interface IExternalCatalogueServiceFactory
{
    ExternalCatalogueService Create(IBlobStorageServiceReadOnly blobStorage);
    ExternalCatalogueService Create(string sourceType);
}
