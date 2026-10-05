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

/// <summary>The trigger endpoint for the ETL pipeline. Nothing here touches the
/// legacy import, which keeps its own controller and its own lock.</summary>
public class EtlImportControllerTests
{
    private readonly Mock<IEtlImportCoordinator> _coordinator = new();
    private readonly EtlImportController _controller;

    public EtlImportControllerTests()
    {
        _controller = Controller();
    }

    private EtlImportController Controller()
    {
        var definitions = new Mock<IDataSetDefinitions>();
        definitions.SetupGet(d => d.All).Returns(StandardDataSetDefinitionsBuilder.Build().All);

        return new EtlImportController(
            _coordinator.Object,
            definitions.Object,
            Mock.Of<ILogger<EtlImportController>>());
    }

    [Fact]
    public async Task StartImport_WhenARebuildClearsNothing_Returns500AndSaysSo()
    {
        _coordinator
            .Setup(c => c.StartAsync(It.IsAny<string>(), It.IsAny<string>(), true, It.IsAny<CancellationToken>()))
            .ReturnsAsync(EtlImportStartResult.RebuildFailed("Could not clear 'raw': in use."));

        var result = await _controller.StartImport("external", null, rebuild: true, CancellationToken.None);

        var error = result.Should().BeOfType<ObjectResult>().Subject;
        error.StatusCode.Should().Be(StatusCodes.Status500InternalServerError);
        error.Value.Should().BeOfType<ErrorResponse>()
            .Which.Message.Should().Contain("Nothing has been cleared");
    }

    [Fact]
    public async Task StartImport_WhenARebuildStopsPartWayThrough_SaysWhichStagesWentFirst()
    {
        _coordinator
            .Setup(c => c.StartAsync(It.IsAny<string>(), It.IsAny<string>(), true, It.IsAny<CancellationToken>()))
            .ReturnsAsync(EtlImportStartResult.RebuildFailed(
                "Could not clear 'views': in use.",
                ["raw", "normalised", "optimised"]));

        var result = await _controller.StartImport("external", null, rebuild: true, CancellationToken.None);

        var error = result.Should().BeOfType<ObjectResult>().Subject;
        error.StatusCode.Should().Be(StatusCodes.Status500InternalServerError);
        error.Value.Should().BeOfType<ErrorResponse>()
            .Which.Message.Should()
                .Contain("raw, normalised, optimised").And
                .Contain("staging area is now incomplete");
    }

    [Fact]
    public async Task StartImport_PassesTheRebuildFlagThrough()
    {
        _coordinator
            .Setup(c => c.StartAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(EtlImportStartResult.Started(Guid.NewGuid()));

        await _controller.StartImport("external", null, rebuild: true, CancellationToken.None);

        _coordinator.Verify(
            c => c.StartAsync("external", null, true, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task StartImport_WhenAccepted_Returns202WithTheImportIdToPoll()
    {
        var importId = Guid.NewGuid();

        _coordinator
            .Setup(c => c.StartAsync("external", "sam_cph_holdings", It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(EtlImportStartResult.Started(importId));

        var result = await _controller.StartImport("external", "sam_cph_holdings", false, CancellationToken.None);

        var accepted = result.Should().BeOfType<AcceptedResult>().Subject;
        accepted.StatusCode.Should().Be(StatusCodes.Status202Accepted);

        var response = accepted.Value.Should().BeOfType<StartEtlImportResponse>().Subject;
        response.ImportId.Should().Be(importId);
        response.Status.Should().Be(nameof(EtlImportStatus.Queued));
    }

    [Fact]
    public async Task StartImport_WithNoDataset_RunsEveryConfiguredDataset()
    {
        _coordinator
            .Setup(c => c.StartAsync(It.IsAny<string>(), null, It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(EtlImportStartResult.Started(Guid.NewGuid()));

        await _controller.StartImport("external", null, false, CancellationToken.None);

        _coordinator.Verify(c => c.StartAsync("external", null, It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task StartImport_WithAnUnknownDataset_Returns400AndDoesNotStartARun()
    {
        var result = await _controller.StartImport("external", "not_a_dataset", false, CancellationToken.None);

        result.Should().BeOfType<BadRequestObjectResult>()
            .Which.Value.Should().BeOfType<KeeperData.Bridge.Controllers.ErrorResponse>()
            .Which.Message.Should().Contain("not_a_dataset");

        _coordinator.Verify(
            c => c.StartAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task StartImport_WithAnInvalidSourceType_Returns400()
    {
        var result = await _controller.StartImport("nowhere", null, false, CancellationToken.None);

        result.Should().BeOfType<BadRequestObjectResult>();

        _coordinator.Verify(
            c => c.StartAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task StartImport_WhenARunIsAlreadyInFlight_Returns409AndNamesTheRunToPollInstead()
    {
        var inFlight = Guid.NewGuid();

        _coordinator
            .Setup(c => c.StartAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(EtlImportStartResult.Conflict(inFlight));

        var result = await _controller.StartImport("external", null, false, CancellationToken.None);

        var conflict = result.Should().BeOfType<ConflictObjectResult>().Subject;
        conflict.StatusCode.Should().Be(StatusCodes.Status409Conflict);
        conflict.Value.Should().BeOfType<EtlImportConflictResponse>()
            .Which.InFlightImportId.Should().Be(inFlight);
    }
}


