using System.Collections.Immutable;
using FluentAssertions;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage.Dtos;
using Moq;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

public class EtlPipelineFactoryTests
{
    private readonly Mock<IExternalCatalogueService> _catalogue = new();

    [Fact]
    public void Create_defines_the_discovery_stage()
    {
        var pipeline = new EtlPipelineFactory(_catalogue.Object).Create();

        pipeline.StageNames.Should().BeEmpty("discovery is the source stage; no downstream stages yet");
    }

    [Fact]
    public async Task Running_the_pipeline_yields_the_discovered_file_sets()
    {
        var definition = new DataSetDefinition("SAM_CPH", "SAM_CPH_{0}", ["cph"], "CHANGE_TYPE", []);

        _catalogue
            .Setup(c => c.GetFileSetsAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(ImmutableList.Create(
                new FileSet(definition, [new EtlFile(new StorageObjectInfo { Container = "external", Key = "SAM_CPH_1.csv" }, DateTimeOffset.UtcNow)])));

        var pipeline = new EtlPipelineFactory(_catalogue.Object).Create();
        var context = new EtlPipelineContext(Guid.NewGuid(), lookbackDays: 0);

        var results = await new PipelineExecutor()
            .RunAsync<DiscoveredFileSet>(pipeline, context, CancellationToken.None);

        results.Should().ContainSingle();
        results[0].Definition.Name.Should().Be("SAM_CPH");
        results[0].Files.Should().ContainSingle();
    }
}
