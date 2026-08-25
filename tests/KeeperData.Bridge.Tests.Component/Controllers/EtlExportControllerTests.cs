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

public class EtlExportControllerTests
{
    private readonly Mock<ICphExportStatusService> _mockStatusService;
    private readonly Mock<IServiceScopeFactory> _mockScopeFactory;
    private readonly Mock<ILogger<EtlExportController>> _mockLogger;
    private readonly EtlExportController _controller;

    public EtlExportControllerTests()
    {
        _mockStatusService = new Mock<ICphExportStatusService>();
        _mockScopeFactory = new Mock<IServiceScopeFactory>();
        _mockLogger = new Mock<ILogger<EtlExportController>>();

        _controller = new EtlExportController(
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

    #region GetExportStatus Tests

    [Fact]
    public async Task GetExportStatus_WhenExportExists_Returns200WithFullStatus()
    {
        var exportId = Guid.NewGuid();
        var requestedAt = DateTime.UtcNow.AddMinutes(-10);
        var startedAt = DateTime.UtcNow.AddMinutes(-9);
        var completedAt = DateTime.UtcNow.AddMinutes(-5);

        var status = new CphExportStatus
        {
            ExportId = exportId,
            Status = ExportStatusType.Succeeded,
            RequestedAt = requestedAt,
            StartedAt = startedAt,
            CompletedAt = completedAt,
            SourceDuckDbPath = "staging/keeper_data_bridge_20260101T120000Z.duckdb",
            SqlitePath = "views/cphs_20260101T120000Z.sqlite",
            RowCount = 1500
        };

        _mockStatusService
            .Setup(s => s.GetAsync(exportId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(status);

        var result = await _controller.GetExportStatus(exportId, CancellationToken.None);

        var okResult = result.Should().BeOfType<OkObjectResult>().Subject;
        okResult.StatusCode.Should().Be(StatusCodes.Status200OK);

        var response = okResult.Value.Should().BeOfType<CphExportStatusResponse>().Subject;
        response.ExportId.Should().Be(exportId);
        response.Status.Should().Be("Succeeded");
        response.RequestedAt.Should().Be(requestedAt);
        response.StartedAt.Should().Be(startedAt);
        response.CompletedAt.Should().Be(completedAt);
        response.SourceDuckDbPath.Should().Be("staging/keeper_data_bridge_20260101T120000Z.duckdb");
        response.SqlitePath.Should().Be("views/cphs_20260101T120000Z.sqlite");
        response.RowCount.Should().Be(1500);
        response.ErrorMessage.Should().BeNull();
    }

    [Fact]
    public async Task GetExportStatus_WhenExportNotFound_Returns404()
    {
        var exportId = Guid.NewGuid();

        _mockStatusService
            .Setup(s => s.GetAsync(exportId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((CphExportStatus?)null);

        var result = await _controller.GetExportStatus(exportId, CancellationToken.None);

        var notFoundResult = result.Should().BeOfType<NotFoundObjectResult>().Subject;
        notFoundResult.StatusCode.Should().Be(StatusCodes.Status404NotFound);

        var response = notFoundResult.Value.Should().BeOfType<CphExportErrorResponse>().Subject;
        response.Message.Should().Contain(exportId.ToString());
        response.ExportId.Should().Be(exportId);
    }

    [Fact]
    public async Task GetExportStatus_WhenRunning_ReturnsRunningStatusWithNoCompletion()
    {
        var exportId = Guid.NewGuid();
        var status = new CphExportStatus
        {
            ExportId = exportId,
            Status = ExportStatusType.Running,
            RequestedAt = DateTime.UtcNow.AddMinutes(-2),
            StartedAt = DateTime.UtcNow.AddMinutes(-1),
            SourceDuckDbPath = "staging/latest"
        };

        _mockStatusService
            .Setup(s => s.GetAsync(exportId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(status);

        var result = await _controller.GetExportStatus(exportId, CancellationToken.None);

        var okResult = result.Should().BeOfType<OkObjectResult>().Subject;
        var response = okResult.Value.Should().BeOfType<CphExportStatusResponse>().Subject;
        response.Status.Should().Be("Running");
        response.StartedAt.Should().NotBeNull();
        response.CompletedAt.Should().BeNull();
        response.SqlitePath.Should().BeNull();
        response.RowCount.Should().BeNull();
    }

    [Fact]
    public async Task GetExportStatus_WhenFailed_ReturnsErrorMessage()
    {
        var exportId = Guid.NewGuid();
        var status = new CphExportStatus
        {
            ExportId = exportId,
            Status = ExportStatusType.Failed,
            RequestedAt = DateTime.UtcNow.AddMinutes(-5),
            StartedAt = DateTime.UtcNow.AddMinutes(-4),
            CompletedAt = DateTime.UtcNow.AddMinutes(-3),
            SourceDuckDbPath = "staging/latest",
            ErrorMessage = "No DuckDB staging files found"
        };

        _mockStatusService
            .Setup(s => s.GetAsync(exportId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(status);

        var result = await _controller.GetExportStatus(exportId, CancellationToken.None);

        var okResult = result.Should().BeOfType<OkObjectResult>().Subject;
        var response = okResult.Value.Should().BeOfType<CphExportStatusResponse>().Subject;
        response.Status.Should().Be("Failed");
        response.ErrorMessage.Should().Be("No DuckDB staging files found");
        response.CompletedAt.Should().NotBeNull();
    }

    [Fact]
    public async Task GetExportStatus_WhenServiceThrows_Returns500()
    {
        var exportId = Guid.NewGuid();

        _mockStatusService
            .Setup(s => s.GetAsync(exportId, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("S3 connection failed"));

        var result = await _controller.GetExportStatus(exportId, CancellationToken.None);

        var statusResult = result.Should().BeOfType<ObjectResult>().Subject;
        statusResult.StatusCode.Should().Be(StatusCodes.Status500InternalServerError);

        var response = statusResult.Value.Should().BeOfType<CphExportErrorResponse>().Subject;
        response.Message.Should().Contain("unexpected error");
    }

    [Fact]
    public async Task GetExportStatus_WhenCancelled_Returns499()
    {
        var exportId = Guid.NewGuid();

        _mockStatusService
            .Setup(s => s.GetAsync(exportId, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new OperationCanceledException());

        var result = await _controller.GetExportStatus(exportId, CancellationToken.None);

        var statusResult = result.Should().BeOfType<ObjectResult>().Subject;
        statusResult.StatusCode.Should().Be(499);
    }

    #endregion

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
            .Setup(sp => sp.GetService(typeof(ILogger<EtlExportController>)))
            .Returns(new Mock<ILogger<EtlExportController>>().Object);

        mockScope.Setup(s => s.ServiceProvider).Returns(mockServiceProvider.Object);

        _mockScopeFactory
            .Setup(f => f.CreateScope())
            .Returns(mockScope.Object);
    }
}
