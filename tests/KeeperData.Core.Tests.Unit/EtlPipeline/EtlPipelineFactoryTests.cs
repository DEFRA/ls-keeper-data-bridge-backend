using FluentAssertions;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using XsvHcdtHelper;
using Xunit;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

[Trait("Category", "Unit")]
public class EtlPipelineFactoryTests
{
    [Fact]
    public void Create_ShouldReturnConfiguredPipeline()
    {
        // Arrange
        var catalogueFactoryMock = new Mock<IExternalCatalogueServiceFactory>();
        var storageProviderMock = new Mock<IEtlPipelineStorageProvider>();
        var hcdtNormaliserMock = new Mock<IXsvHcdtNormaliser>();
        var loggerFactoryMock = new Mock<ILoggerFactory>();
        
        var dummyBlobStorage = new Mock<IBlobStorageService>();
        storageProviderMock.Setup(x => x.ForFolder(It.IsAny<string>())).Returns(dummyBlobStorage.Object);

        var normaliseLoggerMock = new Mock<ILogger<NormaliseStage>>();
        var snapshotLoggerMock = new Mock<ILogger<SnapshotStage>>();

        var sut = new EtlPipelineFactory(
            catalogueFactoryMock.Object,
            storageProviderMock.Object,
            hcdtNormaliserMock.Object,
            TimeProvider.System,
            normaliseLoggerMock.Object,
            snapshotLoggerMock.Object);

        // Act
        var pipeline = sut.Create();

        // Assert
        pipeline.Should().NotBeNull();

        var stageNames = pipeline.GetStageNames();
        stageNames.Should().NotBeEmpty();

        // Verify the pipeline is wired up in the exact correct order
        stageNames.Should().ContainInOrder(
            "discover",
            "decrypt",
            "normalise",
            "snapshot",
            "load-duckdb"
        );
    }
}