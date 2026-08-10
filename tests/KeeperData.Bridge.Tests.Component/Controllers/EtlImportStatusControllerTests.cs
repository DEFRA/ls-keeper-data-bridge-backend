using FluentAssertions;
using KeeperData.Bridge.Controllers;
using KeeperData.Bridge.Models;
using KeeperData.Core.EtlPipeline.Status;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Moq;

namespace KeeperData.Bridge.Tests.Component.Controllers;

/// <summary>The status polling endpoint for the ETL pipeline. Nothing here touches the
/// legacy import, which keeps its own controller and its own lock.</summary>
public class EtlImportStatusControllerTests
{
    private readonly Mock<IEtlImportStatusStore> _statusStore = new();
    private readonly EtlImportStatusController _controller;

    public EtlImportStatusControllerTests()
    {
        _controller = new EtlImportStatusController(_statusStore.Object);
    }

    [Fact]
    public async Task GetImportStatus_ForASucceededRun_ReturnsTheFullPathsQaNeedsToFindTheOutputs()
    {
        var importId = Guid.NewGuid();

        _statusStore
            .Setup(s => s.GetAsync(importId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new EtlImportDocument
            {
                ImportId = importId,
                Status = nameof(EtlImportStatus.Succeeded),
                SourceType = "external",
                Dataset = "sam_cph_holdings",
                DuckDbKey = "keeper_data_bridge_20251115121333.duckdb",
                Datasets =
                [
                    new EtlImportDatasetDocument
                    {
                        Dataset = "sam_cph_holdings",
                        SourceFiles = [new EtlImportSourceFileDocument { Key = "LITP_SAMCPHHOLDING_20251115121333.csv", Size = 1024 }],
                        RawKeys = ["LITP_SAMCPHHOLDING_20251115121333.csv"],
                        NormalisedKeys = ["sam_cph_holdings/LITP_SAMCPHHOLDING_20251115121333.parquet"],
                        SnapshotKey = "sam_cph_holdings/sam_cph_holdings_20251115121333.parquet",
                        RowCount = 3
                    }
                ]
            });

        var result = await _controller.GetImportStatus(importId, CancellationToken.None);

        var response = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlImportStatusResponse>().Subject;

        response.Status.Should().Be(nameof(EtlImportStatus.Succeeded));
        response.DuckDbPath.Should().Be("staging/keeper_data_bridge_20251115121333.duckdb");

        var dataset = response.Datasets.Should().ContainSingle().Subject;
        dataset.RawPaths.Should().Equal("raw/LITP_SAMCPHHOLDING_20251115121333.csv");
        dataset.NormalisedPaths.Should().Equal("normalised/sam_cph_holdings/LITP_SAMCPHHOLDING_20251115121333.parquet");
        dataset.SnapshotPath.Should().Be("snapshots/sam_cph_holdings/sam_cph_holdings_20251115121333.parquet");
        dataset.RowCount.Should().Be(3);
    }

    [Fact]
    public async Task GetImportStatus_ForARunThatHasNotProducedAnythingYet_LeavesThePathsNull()
    {
        var importId = Guid.NewGuid();

        _statusStore
            .Setup(s => s.GetAsync(importId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new EtlImportDocument
            {
                ImportId = importId,
                Status = nameof(EtlImportStatus.Queued),
                SourceType = "external"
            });

        var result = await _controller.GetImportStatus(importId, CancellationToken.None);

        var response = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlImportStatusResponse>().Subject;

        response.DuckDbPath.Should().BeNull();
        response.Datasets.Should().BeEmpty();
    }

    [Fact]
    public async Task GetImportStatus_ForAnUnknownImportId_Returns404()
    {
        _statusStore
            .Setup(s => s.GetAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((EtlImportDocument?)null);

        var result = await _controller.GetImportStatus(Guid.NewGuid(), CancellationToken.None);

        result.Should().BeOfType<NotFoundObjectResult>()
            .Which.StatusCode.Should().Be(StatusCodes.Status404NotFound);
    }
}
