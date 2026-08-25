using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Storage;

namespace KeeperData.Core.ETL.Abstract;

public interface IExternalCatalogueServiceFactory
{
    /// <summary>Creates the superseded per-day-scan catalogue. Reserved for the legacy ingestion pipelines; </summary>
    IExternalCatalogueService CreateLegacy(IBlobStorageServiceReadOnly blobStorage);

    IExternalCatalogueService CreateLegacy(string sourceType);

    /// <summary>Creates the catalogue new code should use: one storage listing per dataset.</summary>
    IExternalCatalogueService Create(IBlobStorageServiceReadOnly blobStorage);

    IExternalCatalogueService Create(string sourceType);

}