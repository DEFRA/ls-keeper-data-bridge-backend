using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Storage;

namespace KeeperData.Core.ETL.Abstract;

public interface IExternalCatalogueServiceFactory
{
    IExternalCatalogueService CreateLegacy(IBlobStorageServiceReadOnly blobStorage);
    IExternalCatalogueService CreateLegacy(string sourceType);
}