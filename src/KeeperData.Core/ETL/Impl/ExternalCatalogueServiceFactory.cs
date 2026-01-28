using System.Diagnostics.CodeAnalysis;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.Storage;

namespace KeeperData.Core.ETL.Impl;

[ExcludeFromCodeCoverage(Justification = "Simple factory wrapper - covered by integration tests.")]
public class ExternalCatalogueServiceFactory(TimeProvider timeProvider, IDataSetDefinitions dataSetDefinitions, IBlobStorageServiceFactory factory) : IExternalCatalogueServiceFactory
{
    public ExternalCatalogueService Create(string sourceType) => new(factory.GetSource(sourceType), timeProvider, dataSetDefinitions);
    public ExternalCatalogueService Create(IBlobStorageServiceReadOnly blobs) => new(blobs, timeProvider, dataSetDefinitions);
}