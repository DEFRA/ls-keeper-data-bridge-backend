using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Storage;

namespace KeeperData.Core.ETL.Impl;

public class ExternalCatalogueServiceFactory(TimeProvider timeProvider, IDataSetDefinitions dataSetDefinitions, IBlobStorageServiceFactory factory) : IExternalCatalogueServiceFactory
{
    public ExternalCatalogueService Create(string sourceType) => new(factory.GetSource(sourceType), timeProvider, dataSetDefinitions);
    public ExternalCatalogueService Create(IBlobStorageServiceReadOnly blobs) => new(blobs, timeProvider, dataSetDefinitions);
}