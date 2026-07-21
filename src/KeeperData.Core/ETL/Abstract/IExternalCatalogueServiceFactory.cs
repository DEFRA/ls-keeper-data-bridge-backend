using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Storage;

namespace KeeperData.Core.ETL.Abstract;

public interface IExternalCatalogueServiceFactory
{
    /// <summary>
    /// Creates the legacy catalogue (slow, multiple listing calls per dataset). Reserved purely for the legacy ingestion pipelines until replaced;
    /// </summary>
    IExternalCatalogueService CreateLegacy(IBlobStorageServiceReadOnly blobStorage);

    /// <summary>
    /// Retained this legacy method
    /// </summary>
    IExternalCatalogueService CreateLegacy(string sourceType);
    
    /// <summary>Creates the new improved catalogue, that calls one bulk storage listing per dataset.</summary>
    IExternalCatalogueService Create(IBlobStorageServiceReadOnly blobStorage);

    /// <inheritdoc cref="Create(IBlobStorageServiceReadOnly)"/>
    IExternalCatalogueService Create(string sourceType);
}