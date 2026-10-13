using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;

namespace KeeperData.Core.ETL.Impl;

[ExcludeFromCodeCoverage(Justification = "Simple factory wrapper - covered by integration tests.")]
public class ExternalCatalogueServiceFactory(
    TimeProvider timeProvider,
    IDataSetDefinitions dataSetDefinitions,
    IBlobStorageServiceFactory factory,
    ILoggerFactory loggerFactory) : IExternalCatalogueServiceFactory
{
    public IExternalCatalogueService CreateLegacy(string sourceType) => new LegacyExternalCatalogueService(factory.GetSource(sourceType), timeProvider, dataSetDefinitions);
    public IExternalCatalogueService CreateLegacy(IBlobStorageServiceReadOnly blobStorage) => new LegacyExternalCatalogueService(blobStorage, timeProvider, dataSetDefinitions);

    public IExternalCatalogueService Create(string sourceType) => Create(factory.GetSource(sourceType));
    public IExternalCatalogueService Create(IBlobStorageServiceReadOnly blobStorage)
        => new BulkListingExternalCatalogueService(
            blobStorage, timeProvider, dataSetDefinitions, loggerFactory.CreateLogger<BulkListingExternalCatalogueService>());
}