using System.Collections.Immutable;
using FluentAssertions;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using Microsoft.Extensions.Logging;
using Moq;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

public class EtlPipelineFactoryTests
{
    private readonly Mock<IExternalCatalogueService> _catalogue = new();
    private readonly Mock<IExternalCatalogueServiceFactory> _catalogueFactory = new();
    private readonly Mock<IBlobStorageService> _internalBlobs = new();
    private readonly Mock<IBlobStorageServiceFactory> _blobFactory = new();

    private EtlPipelineFactory Sut()
    {
        _blobFactory.Setup(f => f.GetSourceInternal()).Returns(_internalBlobs.Object);

        return new EtlPipelineFactory(
            _catalogueFactory.Object,
            _blobFactory.Object,
            Mock.Of<ILogger<ReportDiscoveredFilesStage>>());
    }

    private void GivenCatalogueReturns(DataSetDefinition definition, params EtlFile[] files)
    {
        _catalogue
            .Setup(c => c.GetFileSetsAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(ImmutableList.Create(new FileSet(definition, files)));

        _catalogueFactory.Setup(f => f.Create(It.IsAny<string>())).Returns(_catalogue.Object);
    }

    private static DataSetDefinition Definition(string name) =>
        new(name, $"{name}_{{0}}", ["cph"], "CHANGE_TYPE", []);

    private static EtlFile File(string key) =>
        new(new StorageObjectInfo
        {
            Container = "external",
            Key = key,
            StorageUri = new Uri($"s3://external/{key}")
        }, DateTimeOffset.UtcNow);

    private static Task RunAsync(PipelineDefinition pipeline, EtlPipelineContext context) =>
        new PipelineExecutor(Mock.Of<ILogger<PipelineExecutor>>())
            .RunAsync(pipeline, context, CancellationToken.None);

    [Fact]
    public void Create_defines_the_reporting_stage_after_discovery()
    {
        var pipeline = Sut().Create();

        pipeline.GetStageNames().Should().Equal("report-discovered-files");
    }

    [Fact]
    public async Task Running_the_pipeline_writes_a_discovery_manifest_per_dataset()
    {
        GivenCatalogueReturns(Definition("SAM_CPH"), File("SAM_CPH_1.csv"));

        var runId = Guid.NewGuid();

        await RunAsync(Sut().Create(), new EtlPipelineContext(runId, "external", lookbackDays: 0));

        _internalBlobs.Verify(
            b => b.UploadAsync(
                $"discovery/{runId}/SAM_CPH.json",
                It.IsAny<byte[]>(),
                "application/json",
                null,
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task Running_the_pipeline_writes_nothing_when_no_files_are_discovered()
    {
        GivenCatalogueReturns(Definition("SAM_CPH"));

        await RunAsync(Sut().Create(), new EtlPipelineContext(Guid.NewGuid(), "external", lookbackDays: 0));

        _internalBlobs.Verify(
            b => b.UploadAsync(
                It.IsAny<string>(),
                It.IsAny<byte[]>(),
                It.IsAny<string>(),
                It.IsAny<IReadOnlyDictionary<string, string>>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }
}
