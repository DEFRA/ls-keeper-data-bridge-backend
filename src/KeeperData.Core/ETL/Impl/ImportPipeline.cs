using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Storage;

namespace KeeperData.Core.ETL.Impl;

public class ImportPipeline(IBlobStorageServiceFactory blobStorageServiceFactory, ISourceDataServiceFactory sourceDataServiceFactory,
    DataSetDefinitions dataSetDefinitions)
{
    public async Task StartAsync(string sourceType, CancellationToken ct)
    {
        var sourceBlobs = blobStorageServiceFactory.GetSource(sourceType);
        var sourceDataService = sourceDataServiceFactory.Create(sourceBlobs);
        var files = await sourceDataService.GetFileSetsAsync(ct);
        _ = dataSetDefinitions;

    }

}