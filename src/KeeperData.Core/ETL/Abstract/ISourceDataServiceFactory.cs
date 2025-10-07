using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Storage;

namespace KeeperData.Core.ETL.Abstract;

public interface ISourceDataServiceFactory
{
    SourceDataService Create(IBlobStorageServiceReadOnly blobStorage);
    SourceDataService Create(string sourceType);
}