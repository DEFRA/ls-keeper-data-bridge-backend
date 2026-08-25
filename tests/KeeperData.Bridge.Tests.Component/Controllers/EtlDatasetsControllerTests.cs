using System.Collections.Immutable;
using FluentAssertions;
using KeeperData.Bridge.Controllers;
using KeeperData.Bridge.Models;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using Microsoft.AspNetCore.Mvc;
using Moq;

namespace KeeperData.Bridge.Tests.Component.Controllers;

/// <summary>The dataset list a caller uses to populate a dataset selector with the same names the
/// trigger endpoint accepts.</summary>
public class EtlDatasetsControllerTests
{
    private readonly Mock<IDataSetDefinitions> _dataSetDefinitions = new();
    private readonly EtlDatasetsController _controller;

    public EtlDatasetsControllerTests()
    {
        _controller = new EtlDatasetsController(_dataSetDefinitions.Object);
    }

    [Fact]
    public void GetDatasets_ReturnsEveryConfiguredDefinitionOrderedByName()
    {
        _dataSetDefinitions
            .SetupGet(d => d.All)
            .Returns(
            [
                Definition("sam_cph_holdings"),
                Definition("cts_keepers"),
                Definition("amls2_ports")
            ]);

        var result = _controller.GetDatasets();

        var response = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlDatasetListResponse>().Subject;

        response.Datasets.Select(d => d.Name)
            .Should().Equal("amls2_ports", "cts_keepers", "sam_cph_holdings");
    }

    [Fact]
    public void GetDatasets_ReportsThePrefixAndModeACallerNeedsToExplainWhatWillBeDiscovered()
    {
        _dataSetDefinitions
            .SetupGet(d => d.All)
            .Returns(
            [
                new DataSetDefinition(
                    "sam_cph_holdings",
                    "LITP_SAMCPHHOLDING_",
                    ["cph"],
                    "change_type",
                    [],
                    Format: FileFormat.Hcdt,
                    IngestionMode: DataSetIngestionMode.Delta)
            ]);

        var result = _controller.GetDatasets();

        var dataset = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlDatasetListResponse>()
            .Which.Datasets.Should().ContainSingle().Subject;

        dataset.Name.Should().Be("sam_cph_holdings");
        dataset.FilePrefixFormat.Should().Be("LITP_SAMCPHHOLDING_");
        dataset.Format.Should().Be(nameof(FileFormat.Hcdt));
        dataset.IngestionMode.Should().Be(nameof(DataSetIngestionMode.Delta));
    }

    [Fact]
    public void GetDatasets_WithNothingConfigured_ReturnsAnEmptyListRatherThanFailing()
    {
        _dataSetDefinitions
            .SetupGet(d => d.All)
            .Returns(ImmutableArray<DataSetDefinition>.Empty);

        var result = _controller.GetDatasets();

        result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlDatasetListResponse>()
            .Which.Datasets.Should().BeEmpty();
    }

    private static DataSetDefinition Definition(string name)
        => new(name, $"{name.ToUpperInvariant()}_", ["id"], "change_type", []);
}
