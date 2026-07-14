using System.Collections.Immutable;
using FluentAssertions;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.Storage.Dtos;
using Moq;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

public class DiscoverFilesStageTests
{
    private readonly Mock<IExternalCatalogueService> _catalogue = new();
    private readonly Mock<IExternalCatalogueServiceFactory> _catalogueFactory = new();

    private void GivenCatalogueReturns(params FileSet[] fileSets)
    {
        _catalogue
            .Setup(c => c.GetFileSetsAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(ImmutableList.Create(fileSets));

        _catalogueFactory
            .Setup(f => f.Create(It.IsAny<string>()))
            .Returns(_catalogue.Object);
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

    private async Task<List<DiscoveredFileSet>> RunAsync(EtlPipelineContext context)
    {
        var stage = new DiscoverFilesStage(_catalogueFactory.Object);

        var results = new List<DiscoveredFileSet>();
        await foreach (var item in stage.RunAsync(context, CancellationToken.None))
        {
            results.Add(item);
        }
        return results;
    }

    private static EtlPipelineContext Context(int lookbackDays = 0, string sourceType = "external") =>
        new(Guid.NewGuid(), sourceType, lookbackDays);

    [Fact]
    public async Task Emits_one_file_set_per_dataset_that_has_files()
    {
        var sam = Definition("SAM_CPH");
        var cts = Definition("CTS_KEEPER");

        GivenCatalogueReturns(
            new FileSet(sam, [File("SAM_CPH_1.csv"), File("SAM_CPH_2.csv")]),
            new FileSet(cts, [File("CTS_KEEPER_1.csv")]));

        var results = await RunAsync(Context());

        results.Should().HaveCount(2);
        results[0].Definition.Should().Be(sam);
        results[0].Files.Should().HaveCount(2);
        results[1].Definition.Should().Be(cts);
        results[1].Files.Should().ContainSingle();
    }

    [Fact]
    public async Task Skips_datasets_with_no_files()
    {
        GivenCatalogueReturns(
            new FileSet(Definition("EMPTY"), []),
            new FileSet(Definition("HAS_FILES"), [File("a.csv")]));

        var results = await RunAsync(Context());

        results.Should().ContainSingle();
        results[0].Definition.Name.Should().Be("HAS_FILES");
    }

    [Fact]
    public async Task Passes_the_lookback_days_from_the_context_to_the_catalogue()
    {
        GivenCatalogueReturns();

        await RunAsync(Context(lookbackDays: 7));

        _catalogue.Verify(c => c.GetFileSetsAsync(7, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Creates_the_catalogue_for_the_source_type_of_the_run()
    {
        GivenCatalogueReturns();

        await RunAsync(Context(sourceType: "internal"));

        _catalogueFactory.Verify(f => f.Create("internal"), Times.Once);
    }

    [Fact]
    public async Task Returns_nothing_when_the_catalogue_finds_nothing()
    {
        GivenCatalogueReturns();

        var results = await RunAsync(Context());

        results.Should().BeEmpty();
    }
}
