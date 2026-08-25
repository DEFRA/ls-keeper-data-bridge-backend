using FluentAssertions;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.Storage;
using Moq;

namespace KeeperData.Core.Tests.Unit.ETL;

public class ExternalCatalogueServiceFactoryTests
{
    private const string SourceType = "cts";

    private readonly Mock<IBlobStorageServiceFactory> _blobStorageFactory = new();
    private readonly ExternalCatalogueServiceFactory _factory;

    public ExternalCatalogueServiceFactoryTests()
    {
        _blobStorageFactory
            .Setup(f => f.GetSource(It.IsAny<string>()))
            .Returns(Mock.Of<IBlobStorageServiceReadOnly>());

        _factory = new ExternalCatalogueServiceFactory(TimeProvider.System, Mock.Of<IDataSetDefinitions>(), _blobStorageFactory.Object);
    }

    [Fact]
    public void Create_ForASourceType_ReturnsTheBulkListingCatalogue()
        => _factory.Create(SourceType).Should().BeOfType<BulkListingExternalCatalogueService>();

    [Fact]
    public void Create_ForAGivenBlobStore_ReturnsTheBulkListingCatalogue()
        => _factory.Create(Mock.Of<IBlobStorageServiceReadOnly>()).Should().BeOfType<BulkListingExternalCatalogueService>();

    [Fact]
    public void CreateLegacy_ForASourceType_ReturnsTheLegacyCatalogue()
        => _factory.CreateLegacy(SourceType).Should().BeOfType<LegacyExternalCatalogueService>();

    [Fact]
    public void CreateLegacy_ForAGivenBlobStore_ReturnsTheLegacyCatalogue()
        => _factory.CreateLegacy(Mock.Of<IBlobStorageServiceReadOnly>()).Should().BeOfType<LegacyExternalCatalogueService>();
}
