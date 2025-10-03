using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Storage;

namespace KeeperData.Core.ETL.Impl;

public class SourceDataServiceFactory(TimeProvider timeProvider, IDataSetDefinitions dataSetDefinitions, IBlobStorageServiceFactory factory) : ISourceDataServiceFactory
{
    public SourceDataService Create(string sourceType) => new(factory.GetSource(sourceType), timeProvider, dataSetDefinitions);
    public SourceDataService Create(IBlobStorageServiceReadOnly blobs) => new(blobs, timeProvider, dataSetDefinitions);
}