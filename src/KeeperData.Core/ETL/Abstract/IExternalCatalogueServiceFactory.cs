using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Storage;

namespace KeeperData.Core.ETL.Abstract;

public interface IExternalCatalogueServiceFactory
{
    IExternalCatalogueService Create(IBlobStorageServiceReadOnly blobStorage);
    IExternalCatalogueService Create(string sourceType);
}