using System.Collections.Immutable;
using FluentAssertions;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;
using Moq;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>Source. No input. Output: DiscoveredFile per object.</summary>
public class S3RawFolderSourceTests
{
    private readonly Mock<IExternalCatalogueService> _catalogue = new();
    private readonly Mock<IExternalCatalogueServiceFactory> _catalogueFactory = new();

    private S3RawFolderSource Sut()
    {
        _catalogueFactory.Setup(f => f.Create(It.IsAny<string>())).Returns(_catalogue.Object);
        return new S3RawFolderSource(_catalogueFactory.Object);
    }

    private void GivenSourceFiles(params FileSet[] fileSets) =>
        _catalogue
            .Setup(c => c.GetFileSetsAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(ImmutableList.Create(fileSets));

    private static FileSet FileSetFor(string dataset, params string[] keys) =>
        new(StageRunner.Definition(dataset), [.. keys.Select(k => StageRunner.File(k))]);

    [Fact]
    public async Task Yields_one_discovered_file_per_object()
    {
        GivenSourceFiles(
            FileSetFor("SAM_CPH", "SAM_CPH_1.csv", "SAM_CPH_2.csv"),
            FileSetFor("CTS_KEEPER", "CTS_KEEPER_1.csv"));

        var output = await StageRunner.RunSourceAsync(Sut());

        output.Select(d => d.File.StorageObject.Key)
            .Should().Equal("SAM_CPH_1.csv", "SAM_CPH_2.csv", "CTS_KEEPER_1.csv");
    }

    [Fact]
    public async Task Creates_the_catalogue_for_the_source_type_of_the_run()
    {
        GivenSourceFiles();

        await StageRunner.RunSourceAsync(Sut(), StageRunner.Context(sourceType: "internal"));

        _catalogueFactory.Verify(f => f.Create("internal"), Times.Once);
    }

    [Fact]
    public async Task Passes_the_lookback_days_from_the_run_context()
    {
        GivenSourceFiles();

        await StageRunner.RunSourceAsync(Sut(), StageRunner.Context(lookbackDays: 7));

        _catalogue.Verify(c => c.GetFileSetsAsync(7, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Yields_nothing_when_the_source_is_empty()
    {
        GivenSourceFiles();

        var output = await StageRunner.RunSourceAsync(Sut());

        output.Should().BeEmpty();
    }
}
