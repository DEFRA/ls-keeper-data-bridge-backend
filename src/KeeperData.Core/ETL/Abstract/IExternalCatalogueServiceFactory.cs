using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Storage;

namespace KeeperData.Core.ETL.Abstract;

public interface IExternalCatalogueServiceFactory
{
    /// <summary>Creates the catalogue new code should use: one storage listing per dataset.</summary>
    IExternalCatalogueService Create(IBlobStorageServiceReadOnly blobStorage);

    /// <inheritdoc cref="Create(IBlobStorageServiceReadOnly)"/>
    IExternalCatalogueService Create(string sourceType);

    /// <summary>
    /// Creates the superseded per-day-scan catalogue. Reserved for the legacy ingestion pipelines;
    /// every remaining call site is work still to be migrated onto <see cref="Create(string)"/>.
    /// </summary>
    IExternalCatalogueService CreateLegacy(IBlobStorageServiceReadOnly blobStorage);

    /// <inheritdoc cref="CreateLegacy(IBlobStorageServiceReadOnly)"/>
    IExternalCatalogueService CreateLegacy(string sourceType);
}