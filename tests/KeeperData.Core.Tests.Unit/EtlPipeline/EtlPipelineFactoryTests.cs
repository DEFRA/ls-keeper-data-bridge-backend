using FluentAssertions;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Snapshots;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage;
using KeeperData.Core.Tests.Unit.TestSupport;
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
        var dummyBlobStorage = new Mock<IBlobStorageService>();
        storageProviderMock.Setup(x => x.ForFolder(It.IsAny<string>())).Returns(dummyBlobStorage.Object);

        var normaliseLoggerMock = new Mock<ILogger<NormaliseStage>>();
        var snapshotLoggerMock = new Mock<ILogger<SnapshotStage>>();
        var decryptStage = AutoMocked.Instance<DecryptStage>();
        var normaliseStage = new NormaliseStage(
            storageProviderMock.Object,
            hcdtNormaliserMock.Object,
            normaliseLoggerMock.Object);
        var snapshotStage = new SnapshotStage(
            storageProviderMock.Object,
            new Mock<IDeltaMergeEngine>().Object,
            snapshotLoggerMock.Object);
        var loadDuckDbStage = AutoMocked.Instance<LoadDuckDbStage>();
        var exportSqliteStage = AutoMocked.Instance<ExportSqliteStage>();

        var sut = new EtlPipelineFactory(
            catalogueFactoryMock.Object,
            decryptStage,
            normaliseStage,
            snapshotStage,
            loadDuckDbStage,
            exportSqliteStage);

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
            "load-duckdb",
            "export-sqlite"
        );
    }
}
