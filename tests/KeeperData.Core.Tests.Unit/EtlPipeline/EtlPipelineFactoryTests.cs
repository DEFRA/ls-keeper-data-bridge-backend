using System.Collections.Immutable;
using FluentAssertions;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Payloads;
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

    [Fact]
    public void Create_defines_the_reporting_stage_after_discovery()
    {
        var pipeline = Sut().Create();

        pipeline.GetStageNames().Should().Equal("report-discovered-files");
    }

    [Fact]
    public async Task Running_the_pipeline_yields_the_discovered_file_sets()
    {
        var definition = new DataSetDefinition("SAM_CPH", "SAM_CPH_{0}", ["cph"], "CHANGE_TYPE", []);
        var file = new EtlFile(
            new StorageObjectInfo
            {
                Container = "external",
                Key = "SAM_CPH_1.csv",
                StorageUri = new Uri("s3://external/SAM_CPH_1.csv")
            },
            DateTimeOffset.UtcNow);

        _catalogue
            .Setup(c => c.GetFileSetsAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(ImmutableList.Create(new FileSet(definition, [file])));

        _catalogueFactory.Setup(f => f.Create(It.IsAny<string>())).Returns(_catalogue.Object);

        var pipeline = Sut().Create();
        var context = new EtlPipelineContext(Guid.NewGuid(), "external", lookbackDays: 0);

        var results = await new PipelineExecutor(Mock.Of<ILogger<PipelineExecutor>>())
            .RunAsync<DiscoveredFileSet>(pipeline, context, CancellationToken.None);

        results.Should().ContainSingle();
        results[0].Definition.Name.Should().Be("SAM_CPH");
        results[0].Files.Should().ContainSingle();
    }

    [Fact]
    public async Task Running_the_pipeline_writes_a_discovery_manifest_to_the_internal_bucket()
    {
        var definition = new DataSetDefinition("SAM_CPH", "SAM_CPH_{0}", ["cph"], "CHANGE_TYPE", []);
        var file = new EtlFile(
            new StorageObjectInfo
            {
                Container = "external",
                Key = "SAM_CPH_1.csv",
                StorageUri = new Uri("s3://external/SAM_CPH_1.csv")
            },
            DateTimeOffset.UtcNow);

        _catalogue
            .Setup(c => c.GetFileSetsAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(ImmutableList.Create(new FileSet(definition, [file])));

        _catalogueFactory.Setup(f => f.Create(It.IsAny<string>())).Returns(_catalogue.Object);

        var runId = Guid.NewGuid();
        var pipeline = Sut().Create();
        var context = new EtlPipelineContext(runId, "external", lookbackDays: 0);

        await new PipelineExecutor(Mock.Of<ILogger<PipelineExecutor>>())
            .RunAsync(pipeline, context, CancellationToken.None);

        _internalBlobs.Verify(
            b => b.UploadAsync(
                $"discovery/{runId}/SAM_CPH.json",
                It.IsAny<byte[]>(),
                "application/json",
                null,
                It.IsAny<CancellationToken>()),
            Times.Once);
    }
}
