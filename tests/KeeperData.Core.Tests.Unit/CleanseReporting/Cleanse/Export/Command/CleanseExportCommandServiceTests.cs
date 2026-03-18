using FluentAssertions;
using KeeperData.Core.Locking;
using KeeperData.Core.Reports.Cleanse.Export.Command;
using KeeperData.Core.Reports.Cleanse.Export.Command.Abstract;
using KeeperData.Core.Reports.Cleanse.Export.Command.AggregateRoots;
using KeeperData.Core.Reports.Cleanse.Export.Command.Domain;
using KeeperData.Core.Reports.Cleanse.Export.Operations.Abstract;
using KeeperData.Core.Reports.Cleanse.Export.Operations.Dtos;
using KeeperData.Core.Storage;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace KeeperData.Core.Tests.Unit.CleanseReporting.Cleanse.Export.Command;

public class CleanseExportCommandServiceTests
{
    private readonly Mock<ICleanseReportExportCommandService> _exportServiceMock = new();
    private readonly Mock<ICleanseExportOperationRepository> _repositoryMock = new();
    private readonly Mock<ICleanseExportOperationQueries> _queriesMock = new();
    private readonly Mock<IDistributedLock> _lockMock = new();
    private readonly Mock<IBlobStorageServiceFactory> _blobFactoryMock = new();
    private readonly CleanseExportCommandService _sut;

    public CleanseExportCommandServiceTests()
    {
        _sut = new CleanseExportCommandService(
            _exportServiceMock.Object,
            _repositoryMock.Object,
            _queriesMock.Object,
            _lockMock.Object,
            _blobFactoryMock.Object,
            NullLogger<CleanseExportCommandService>.Instance);
    }

    [Fact]
    public async Task StartFullExportAsync_WhenLockNotAcquired_ShouldReturnNull()
    {
        _lockMock.Setup(l => l.TryAcquireAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((IDistributedLockHandle?)null);

        var result = await _sut.StartFullExportAsync(CancellationToken.None);

        result.Should().BeNull();
        _repositoryMock.Verify(r => r.CreateAsync(It.IsAny<CleanseExportOperation>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task StartFullExportAsync_WhenLockAcquired_ShouldCreateOperationAndReturnDto()
    {
        var lockHandleMock = new Mock<IDistributedLockHandle>();
        lockHandleMock.Setup(h => h.TryRenewAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        _lockMock.Setup(l => l.TryAcquireAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(lockHandleMock.Object);

        var expectedDto = new CleanseExportOperationDto
        {
            Id = "test-id",
            Status = CleanseExportStatus.Pending.ToString(),
            StartedAtUtc = DateTime.UtcNow
        };

        _queriesMock.Setup(q => q.GetOperationAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedDto);

        var result = await _sut.StartFullExportAsync(CancellationToken.None);

        result.Should().NotBeNull();
        result!.Status.Should().Be(CleanseExportStatus.Pending.ToString());
        _repositoryMock.Verify(r => r.CreateAsync(It.IsAny<CleanseExportOperation>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task GetExportOperationAsync_ShouldDelegateToQueries()
    {
        var expectedDto = new CleanseExportOperationDto
        {
            Id = "export-1",
            Status = CleanseExportStatus.Completed.ToString()
        };
        _queriesMock.Setup(q => q.GetOperationAsync("export-1", It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedDto);

        var result = await _sut.GetExportOperationAsync("export-1");

        result.Should().NotBeNull();
        result!.Id.Should().Be("export-1");
    }

    [Fact]
    public async Task GetExportOperationsAsync_ShouldDelegateToQueries()
    {
        var expected = new List<CleanseExportOperationSummaryDto>
        {
            new() { Id = "e1", Status = "Completed" },
            new() { Id = "e2", Status = "Running" }
        };
        _queriesMock.Setup(q => q.GetOperationsAsync(0, 10, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expected);

        var result = await _sut.GetExportOperationsAsync(0, 10);

        result.Should().HaveCount(2);
    }

    [Fact]
    public async Task RegenerateExportUrlAsync_WhenOperationNotFound_ShouldReturnError()
    {
        _repositoryMock.Setup(r => r.GetByIdAsync("missing", It.IsAny<CancellationToken>()))
            .ReturnsAsync((CleanseExportOperation?)null);

        var result = await _sut.RegenerateExportUrlAsync("missing");

        result.Success.Should().BeFalse();
        result.Error.Should().Contain("not found");
    }

    [Fact]
    public async Task RegenerateExportUrlAsync_WhenNoReportObjectKey_ShouldReturnError()
    {
        var operation = CleanseExportOperation.Create();
        _repositoryMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(operation);

        var result = await _sut.RegenerateExportUrlAsync(operation.Id);

        result.Success.Should().BeFalse();
        result.Error.Should().Contain("does not have a report file");
    }

    [Fact]
    public async Task RegenerateExportUrlAsync_WhenReportExists_ShouldReturnNewUrl()
    {
        var operation = CleanseExportOperation.Create();
        operation.SetReportDetails("report.zip", "https://old-url");

        _repositoryMock.Setup(r => r.GetByIdAsync(operation.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(operation);

        var blobServiceMock = new Mock<IBlobStorageService>();
        blobServiceMock.Setup(b => b.GeneratePresignedUrl("report.zip", It.IsAny<TimeSpan?>()))
            .Returns("https://new-url");
        _blobFactoryMock.Setup(f => f.GetCleanseReportsBlobService())
            .Returns(blobServiceMock.Object);

        var result = await _sut.RegenerateExportUrlAsync(operation.Id);

        result.Success.Should().BeTrue();
        result.ReportUrl.Should().Be("https://new-url");
        result.ObjectKey.Should().Be("report.zip");
        _repositoryMock.Verify(r => r.UpdateAsync(
            It.Is<CleanseExportOperation>(o => o.ReportUrl == "https://new-url"),
            It.IsAny<CancellationToken>()), Times.Once);
    }
}
