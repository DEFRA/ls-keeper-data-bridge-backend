using FluentAssertions;
using KeeperData.Bridge.Controllers;
using KeeperData.Bridge.Models;
using KeeperData.Bridge.Worker.Coordination;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Status;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Moq;

namespace KeeperData.Bridge.Tests.Component.Controllers;

/// <summary>The trigger and status endpoints for the file-based pipeline. Nothing here touches the
/// legacy import, which keeps its own controller and its own lock.</summary>
public class FileBasedImportControllerTests
{
    private readonly Mock<IFileBasedImportCoordinator> _coordinator = new();
    private readonly Mock<IEtlImportStatusStore> _statusStore = new();
    private readonly FileBasedImportController _controller;

    public FileBasedImportControllerTests()
    {
        var definitions = new Mock<IDataSetDefinitions>();
        definitions.SetupGet(d => d.All).Returns(StandardDataSetDefinitionsBuilder.Build().All);

        _controller = new FileBasedImportController(
            _coordinator.Object,
            _statusStore.Object,
            definitions.Object,
            Mock.Of<ILogger<FileBasedImportController>>());
    }

    [Fact]
    public async Task StartImport_WhenAccepted_Returns202WithTheImportIdToPoll()
    {
        var importId = Guid.NewGuid();

        _coordinator
            .Setup(c => c.StartAsync("external", "sam_cph_holdings", It.IsAny<CancellationToken>()))
            .ReturnsAsync(FileBasedImportStartResult.Started(importId));

        var result = await _controller.StartImport("external", "sam_cph_holdings", CancellationToken.None);

        var accepted = result.Should().BeOfType<AcceptedResult>().Subject;
        accepted.StatusCode.Should().Be(StatusCodes.Status202Accepted);

        var response = accepted.Value.Should().BeOfType<StartFileBasedImportResponse>().Subject;
        response.ImportId.Should().Be(importId);
        response.Status.Should().Be(nameof(EtlImportStatus.Queued));
    }

    [Fact]
    public async Task StartImport_WithNoDataset_RunsEveryConfiguredDataset()
    {
        _coordinator
            .Setup(c => c.StartAsync(It.IsAny<string>(), null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(FileBasedImportStartResult.Started(Guid.NewGuid()));

        await _controller.StartImport("external", null, CancellationToken.None);

        _coordinator.Verify(c => c.StartAsync("external", null, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StartImport_WithAnUnknownDataset_Returns400AndDoesNotStartARun()
    {
        var result = await _controller.StartImport("external", "not_a_dataset", CancellationToken.None);

        result.Should().BeOfType<BadRequestObjectResult>()
            .Which.Value.Should().BeOfType<KeeperData.Bridge.Controllers.ErrorResponse>()
            .Which.Message.Should().Contain("not_a_dataset");

        _coordinator.Verify(
            c => c.StartAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task StartImport_WithAnInvalidSourceType_Returns400()
    {
        var result = await _controller.StartImport("nowhere", null, CancellationToken.None);

        result.Should().BeOfType<BadRequestObjectResult>();

        _coordinator.Verify(
            c => c.StartAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task StartImport_WhenARunIsAlreadyInFlight_Returns409AndNamesTheRunToPollInstead()
    {
        var inFlight = Guid.NewGuid();

        _coordinator
            .Setup(c => c.StartAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(FileBasedImportStartResult.Conflict(inFlight));

        var result = await _controller.StartImport("external", null, CancellationToken.None);

        var conflict = result.Should().BeOfType<ConflictObjectResult>().Subject;
        conflict.StatusCode.Should().Be(StatusCodes.Status409Conflict);
        conflict.Value.Should().BeOfType<FileBasedImportConflictResponse>()
            .Which.InFlightImportId.Should().Be(inFlight);
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
            .Which.Value.Should().BeOfType<FileBasedImportStatusResponse>().Subject;

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
            .Which.Value.Should().BeOfType<FileBasedImportStatusResponse>().Subject;

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
