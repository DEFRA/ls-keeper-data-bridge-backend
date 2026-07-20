using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Storage;

namespace KeeperData.Core.ETL.Impl;

[ExcludeFromCodeCoverage(Justification = "Simple factory wrapper - covered by integration tests.")]
public class ExternalCatalogueServiceFactory(TimeProvider timeProvider, IDataSetDefinitions dataSetDefinitions, IBlobStorageServiceFactory factory) : IExternalCatalogueServiceFactory
{
    public IExternalCatalogueService Create(string sourceType) => new LegacyExternalCatalogueService(factory.GetSource(sourceType), timeProvider, dataSetDefinitions);
    public IExternalCatalogueService Create(IBlobStorageServiceReadOnly blobStorage) => new LegacyExternalCatalogueService(blobStorage, timeProvider, dataSetDefinitions);
}