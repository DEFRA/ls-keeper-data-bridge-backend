using System.Text.Json;
using System.Text.Json.Serialization;
using FluentAssertions;
using KeeperData.Core.ETL.Models;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using KeeperData.Core.Telemetry;
using KeeperData.Infrastructure.ETL;
using Microsoft.Extensions.Logging;
using Moq;

namespace KeeperData.Infrastructure.Tests.Unit.ETL;

public class CphExportStatusServiceTests
{
    private readonly Mock<IBlobStorageServiceFactory> _storageFactoryMock;
    private readonly Mock<IBlobStorageService> _internalStorageMock;
    private readonly Mock<IApplicationMetrics> _metricsMock;
    private readonly Mock<ILogger<CphExportStatusService>> _loggerMock;
    private readonly CphExportStatusService _sut;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };

    public CphExportStatusServiceTests()
    {
        _storageFactoryMock = new Mock<IBlobStorageServiceFactory>();
        _internalStorageMock = new Mock<IBlobStorageService>();
        _metricsMock = new Mock<IApplicationMetrics>();
        _loggerMock = new Mock<ILogger<CphExportStatusService>>();

        _storageFactoryMock
            .Setup(f => f.GetSourceInternal())
            .Returns(_internalStorageMock.Object);

        _sut = new CphExportStatusService(_storageFactoryMock.Object, _metricsMock.Object, _loggerMock.Object);
    }

    [Fact]
    public async Task CreateAsync_PersistsStatusAsJsonToS3()
    {
        var exportId = Guid.NewGuid();
        var sourcePath = "staging/keeper_data_bridge_20260623T120000Z.duckdb";

        var result = await _sut.CreateAsync(exportId, sourcePath);

        result.ExportId.Should().Be(exportId);
        result.Status.Should().Be(ExportStatusType.Queued);
        result.SourceDuckDbPath.Should().Be(sourcePath);
        result.RequestedAt.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));

        _internalStorageMock.Verify(
            s => s.UploadAsync(
                $"exports/cphs/{exportId}.json",
                It.IsAny<byte[]>(),
                "application/json",
                null,
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task CreateAsync_EmitsQueuedMetric()
    {
        var exportId = Guid.NewGuid();

        await _sut.CreateAsync(exportId, "staging/test.duckdb");

        _metricsMock.Verify(m => m.RecordCount("export.queued", 1), Times.Once);
    }

    [Fact]
    public async Task GetAsync_ReturnsDeserializedStatus()
    {
        var exportId = Guid.NewGuid();
        var status = new CphExportStatus
        {
            ExportId = exportId,
            Status = ExportStatusType.Succeeded,
            RequestedAt = DateTime.UtcNow.AddMinutes(-5),
            StartedAt = DateTime.UtcNow.AddMinutes(-4),
            CompletedAt = DateTime.UtcNow.AddMinutes(-1),
            SourceDuckDbPath = "staging/test.duckdb",
            SqlitePath = "views/cphs_20260623T120000Z.sqlite",
            RowCount = 450
        };

        var json = JsonSerializer.SerializeToUtf8Bytes(status, JsonOptions);

        _internalStorageMock
            .Setup(s => s.ExistsAsync($"exports/cphs/{exportId}.json", It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        _internalStorageMock
            .Setup(s => s.DownloadAsync($"exports/cphs/{exportId}.json", It.IsAny<CancellationToken>()))
            .ReturnsAsync(json);

        var result = await _sut.GetAsync(exportId);

        result.Should().NotBeNull();
        result!.ExportId.Should().Be(exportId);
        result.Status.Should().Be(ExportStatusType.Succeeded);
        result.RowCount.Should().Be(450);
        result.SqlitePath.Should().Be("views/cphs_20260623T120000Z.sqlite");
    }

    [Fact]
    public async Task GetAsync_ReturnsNullWhenNotFound()
    {
        var exportId = Guid.NewGuid();

        _internalStorageMock
            .Setup(s => s.ExistsAsync($"exports/cphs/{exportId}.json", It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        var result = await _sut.GetAsync(exportId);

        result.Should().BeNull();
    }

    [Fact]
    public async Task UpdateAsync_PersistsUpdatedStatusToS3()
    {
        var exportId = Guid.NewGuid();
        var status = new CphExportStatus
        {
            ExportId = exportId,
            Status = ExportStatusType.Running,
            RequestedAt = DateTime.UtcNow.AddMinutes(-2),
            StartedAt = DateTime.UtcNow,
            SourceDuckDbPath = "staging/test.duckdb"
        };

        await _sut.UpdateAsync(status);

        _internalStorageMock.Verify(
            s => s.UploadAsync(
                $"exports/cphs/{exportId}.json",
                It.IsAny<byte[]>(),
                "application/json",
                null,
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task UpdateAsync_EmitsSucceededMetric()
    {
        var status = new CphExportStatus
        {
            ExportId = Guid.NewGuid(),
            Status = ExportStatusType.Succeeded,
            RequestedAt = DateTime.UtcNow,
            SourceDuckDbPath = "staging/test.duckdb"
        };

        await _sut.UpdateAsync(status);

        _metricsMock.Verify(m => m.RecordCount("export.succeeded", 1), Times.Once);
    }

    [Fact]
    public async Task UpdateAsync_EmitsFailedMetric()
    {
        var status = new CphExportStatus
        {
            ExportId = Guid.NewGuid(),
            Status = ExportStatusType.Failed,
            RequestedAt = DateTime.UtcNow,
            SourceDuckDbPath = "staging/test.duckdb",
            ErrorMessage = "Something went wrong"
        };

        await _sut.UpdateAsync(status);

        _metricsMock.Verify(m => m.RecordCount("export.failed", 1), Times.Once);
    }

    [Fact]
    public async Task GetLatestRunningAsync_ReturnsRunningExport()
    {
        var runningId = Guid.NewGuid();
        var runningStatus = new CphExportStatus
        {
            ExportId = runningId,
            Status = ExportStatusType.Running,
            RequestedAt = DateTime.UtcNow.AddMinutes(-2),
            StartedAt = DateTime.UtcNow.AddMinutes(-1),
            SourceDuckDbPath = "staging/test.duckdb"
        };

        var objects = new List<StorageObjectInfo>
        {
            new() { Key = $"exports/cphs/{runningId}.json", Container = "bucket", Size = 100, LastModified = DateTimeOffset.UtcNow, StorageUri = new Uri($"s3://bucket/exports/cphs/{runningId}.json") }
        };

        _internalStorageMock
            .Setup(s => s.ListAsync("exports/cphs/", It.IsAny<CancellationToken>()))
            .ReturnsAsync(objects);

        _internalStorageMock
            .Setup(s => s.DownloadAsync($"exports/cphs/{runningId}.json", It.IsAny<CancellationToken>()))
            .ReturnsAsync(JsonSerializer.SerializeToUtf8Bytes(runningStatus, JsonOptions));

        var result = await _sut.GetLatestRunningAsync();

        result.Should().NotBeNull();
        result!.ExportId.Should().Be(runningId);
        result.Status.Should().Be(ExportStatusType.Running);
    }

    [Fact]
    public async Task GetLatestRunningAsync_ReturnsQueuedExport()
    {
        var queuedId = Guid.NewGuid();
        var queuedStatus = new CphExportStatus
        {
            ExportId = queuedId,
            Status = ExportStatusType.Queued,
            RequestedAt = DateTime.UtcNow.AddMinutes(-1),
            SourceDuckDbPath = "staging/test.duckdb"
        };

        var objects = new List<StorageObjectInfo>
        {
            new() { Key = $"exports/cphs/{queuedId}.json", Container = "bucket", Size = 100, LastModified = DateTimeOffset.UtcNow, StorageUri = new Uri($"s3://bucket/exports/cphs/{queuedId}.json") }
        };

        _internalStorageMock
            .Setup(s => s.ListAsync("exports/cphs/", It.IsAny<CancellationToken>()))
            .ReturnsAsync(objects);

        _internalStorageMock
            .Setup(s => s.DownloadAsync($"exports/cphs/{queuedId}.json", It.IsAny<CancellationToken>()))
            .ReturnsAsync(JsonSerializer.SerializeToUtf8Bytes(queuedStatus, JsonOptions));

        var result = await _sut.GetLatestRunningAsync();

        result.Should().NotBeNull();
        result!.Status.Should().Be(ExportStatusType.Queued);
    }

    [Fact]
    public async Task GetLatestRunningAsync_ReturnsNullWhenAllCompleted()
    {
        var completedId = Guid.NewGuid();
        var completedStatus = new CphExportStatus
        {
            ExportId = completedId,
            Status = ExportStatusType.Succeeded,
            RequestedAt = DateTime.UtcNow.AddMinutes(-10),
            CompletedAt = DateTime.UtcNow.AddMinutes(-5),
            SourceDuckDbPath = "staging/test.duckdb",
            RowCount = 100
        };

        var objects = new List<StorageObjectInfo>
        {
            new() { Key = $"exports/cphs/{completedId}.json", Container = "bucket", Size = 100, LastModified = DateTimeOffset.UtcNow, StorageUri = new Uri($"s3://bucket/exports/cphs/{completedId}.json") }
        };

        _internalStorageMock
            .Setup(s => s.ListAsync("exports/cphs/", It.IsAny<CancellationToken>()))
            .ReturnsAsync(objects);

        _internalStorageMock
            .Setup(s => s.DownloadAsync($"exports/cphs/{completedId}.json", It.IsAny<CancellationToken>()))
            .ReturnsAsync(JsonSerializer.SerializeToUtf8Bytes(completedStatus, JsonOptions));

        var result = await _sut.GetLatestRunningAsync();

        result.Should().BeNull();
    }

    [Fact]
    public async Task GetLatestRunningAsync_ReturnsNullWhenNoExports()
    {
        _internalStorageMock
            .Setup(s => s.ListAsync("exports/cphs/", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<StorageObjectInfo>());

        var result = await _sut.GetLatestRunningAsync();

        result.Should().BeNull();
    }
}
