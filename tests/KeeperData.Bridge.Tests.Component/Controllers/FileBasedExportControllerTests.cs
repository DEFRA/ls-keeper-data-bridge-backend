using FluentAssertions;
using KeeperData.Bridge.Controllers;
using KeeperData.Core.ETL.Export;
using KeeperData.Core.ETL.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;

namespace KeeperData.Bridge.Tests.Component.Controllers;

public class FileBasedExportControllerTests
{
    private readonly Mock<ICphExportStatusService> _mockStatusService;
    private readonly Mock<IServiceScopeFactory> _mockScopeFactory;
    private readonly Mock<ILogger<FileBasedExportController>> _mockLogger;
    private readonly FileBasedExportController _controller;

    public FileBasedExportControllerTests()
    {
        _mockStatusService = new Mock<ICphExportStatusService>();
        _mockScopeFactory = new Mock<IServiceScopeFactory>();
        _mockLogger = new Mock<ILogger<FileBasedExportController>>();

        _controller = new FileBasedExportController(
            _mockStatusService.Object,
            _mockScopeFactory.Object,
            _mockLogger.Object);
    }

    [Fact]
    public async Task TriggerCphExport_WhenNoRunningExport_Returns202Accepted()
    {
        var exportId = Guid.NewGuid();
        var status = new CphExportStatus
        {
            ExportId = exportId,
            Status = ExportStatusType.Queued,
            RequestedAt = DateTime.UtcNow,
            SourceDuckDbPath = "staging/latest"
        };

        _mockStatusService
            .Setup(s => s.GetLatestRunningAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((CphExportStatus?)null);

        _mockStatusService
            .Setup(s => s.CreateAsync(It.IsAny<Guid>(), "staging/latest", It.IsAny<CancellationToken>()))
            .ReturnsAsync(status);

        SetupScopeFactory();

        var result = await _controller.TriggerCphExport(CancellationToken.None);

        var acceptedResult = result.Should().BeOfType<AcceptedResult>().Subject;
        acceptedResult.StatusCode.Should().Be(StatusCodes.Status202Accepted);

        var response = acceptedResult.Value.Should().BeOfType<CphExportAcceptedResponse>().Subject;
        response.Status.Should().Be("Queued");
        response.Message.Should().Contain("background");
    }

    [Fact]
    public async Task TriggerCphExport_WhenExportAlreadyRunning_Returns409Conflict()
    {
        var runningExport = new CphExportStatus
        {
            ExportId = Guid.NewGuid(),
            Status = ExportStatusType.Running,
            RequestedAt = DateTime.UtcNow.AddMinutes(-5),
            SourceDuckDbPath = "staging/keeper_data_bridge_20260101T120000Z.duckdb"
        };

        _mockStatusService
            .Setup(s => s.GetLatestRunningAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(runningExport);

        var result = await _controller.TriggerCphExport(CancellationToken.None);

        var conflictResult = result.Should().BeOfType<ConflictObjectResult>().Subject;
        conflictResult.StatusCode.Should().Be(StatusCodes.Status409Conflict);

        var response = conflictResult.Value.Should().BeOfType<CphExportErrorResponse>().Subject;
        response.Message.Should().Contain("already running");
        response.ExportId.Should().Be(runningExport.ExportId);
    }

    [Fact]
    public async Task TriggerCphExport_WhenExportAlreadyQueued_Returns409Conflict()
    {
        var queuedExport = new CphExportStatus
        {
            ExportId = Guid.NewGuid(),
            Status = ExportStatusType.Queued,
            RequestedAt = DateTime.UtcNow.AddMinutes(-1),
            SourceDuckDbPath = "staging/latest"
        };

        _mockStatusService
            .Setup(s => s.GetLatestRunningAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(queuedExport);

        var result = await _controller.TriggerCphExport(CancellationToken.None);

        var conflictResult = result.Should().BeOfType<ConflictObjectResult>().Subject;
        var response = conflictResult.Value.Should().BeOfType<CphExportErrorResponse>().Subject;
        response.Message.Should().Contain("already queued");
    }

    [Fact]
    public async Task TriggerCphExport_CreatesStatusWithQueuedState()
    {
        _mockStatusService
            .Setup(s => s.GetLatestRunningAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((CphExportStatus?)null);

        _mockStatusService
            .Setup(s => s.CreateAsync(It.IsAny<Guid>(), "staging/latest", It.IsAny<CancellationToken>()))
            .ReturnsAsync((Guid id, string path, CancellationToken _) => new CphExportStatus
            {
                ExportId = id,
                Status = ExportStatusType.Queued,
                RequestedAt = DateTime.UtcNow,
                SourceDuckDbPath = path
            });

        SetupScopeFactory();

        await _controller.TriggerCphExport(CancellationToken.None);

        _mockStatusService.Verify(
            s => s.CreateAsync(It.IsAny<Guid>(), "staging/latest", It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task TriggerCphExport_WhenStatusServiceThrows_Returns500()
    {
        _mockStatusService
            .Setup(s => s.GetLatestRunningAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("S3 unavailable"));

        var result = await _controller.TriggerCphExport(CancellationToken.None);

        var statusResult = result.Should().BeOfType<ObjectResult>().Subject;
        statusResult.StatusCode.Should().Be(StatusCodes.Status500InternalServerError);

        var response = statusResult.Value.Should().BeOfType<CphExportErrorResponse>().Subject;
        response.Message.Should().Contain("unexpected error");
    }

    [Fact]
    public async Task TriggerCphExport_AcceptedResponse_ContainsExportId()
    {
        _mockStatusService
            .Setup(s => s.GetLatestRunningAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((CphExportStatus?)null);

        _mockStatusService
            .Setup(s => s.CreateAsync(It.IsAny<Guid>(), "staging/latest", It.IsAny<CancellationToken>()))
            .ReturnsAsync((Guid id, string path, CancellationToken _) => new CphExportStatus
            {
                ExportId = id,
                Status = ExportStatusType.Queued,
                RequestedAt = DateTime.UtcNow,
                SourceDuckDbPath = path
            });

        SetupScopeFactory();

        var result = await _controller.TriggerCphExport(CancellationToken.None);

        var acceptedResult = result.Should().BeOfType<AcceptedResult>().Subject;
        acceptedResult.StatusCode.Should().Be(StatusCodes.Status202Accepted);
        var response = acceptedResult.Value.Should().BeOfType<CphExportAcceptedResponse>().Subject;
        response.ExportId.Should().NotBeEmpty();
        response.RequestedAt.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task TriggerCphExport_WhenCancelled_Returns499()
    {
        var cts = new CancellationTokenSource();
        cts.Cancel();

        _mockStatusService
            .Setup(s => s.GetLatestRunningAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new OperationCanceledException());

        var result = await _controller.TriggerCphExport(cts.Token);

        var statusResult = result.Should().BeOfType<ObjectResult>().Subject;
        statusResult.StatusCode.Should().Be(499);
    }

    private void SetupScopeFactory()
    {
        var mockScope = new Mock<IServiceScope>();
        var mockServiceProvider = new Mock<IServiceProvider>();

        mockServiceProvider
            .Setup(sp => sp.GetService(typeof(ICphExportService)))
            .Returns(new Mock<ICphExportService>().Object);
        mockServiceProvider
            .Setup(sp => sp.GetService(typeof(ICphExportStatusService)))
            .Returns(new Mock<ICphExportStatusService>().Object);
        mockServiceProvider
            .Setup(sp => sp.GetService(typeof(ILogger<FileBasedExportController>)))
            .Returns(new Mock<ILogger<FileBasedExportController>>().Object);

        mockScope.Setup(s => s.ServiceProvider).Returns(mockServiceProvider.Object);

        _mockScopeFactory
            .Setup(f => f.CreateScope())
            .Returns(mockScope.Object);
    }
}
