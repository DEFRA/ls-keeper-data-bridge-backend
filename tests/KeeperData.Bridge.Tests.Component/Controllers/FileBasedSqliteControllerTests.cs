using FluentAssertions;
using KeeperData.Bridge.Controllers;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Moq;

namespace KeeperData.Bridge.Tests.Component.Controllers;

public class FileBasedSqliteControllerTests
{
    private readonly Mock<IBlobStorageServiceFactory> _mockStorageFactory;
    private readonly Mock<IBlobStorageService> _mockStorageService;
    private readonly Mock<ILogger<FileBasedSqliteController>> _mockLogger;
    private readonly FileBasedSqliteController _controller;

    public FileBasedSqliteControllerTests()
    {
        _mockStorageFactory = new Mock<IBlobStorageServiceFactory>();
        _mockStorageService = new Mock<IBlobStorageService>();
        _mockLogger = new Mock<ILogger<FileBasedSqliteController>>();

        _mockStorageFactory
            .Setup(f => f.GetSourceInternal())
            .Returns(_mockStorageService.Object);

        _controller = new FileBasedSqliteController(
            _mockStorageFactory.Object,
            _mockLogger.Object);
    }

    [Fact]
    public async Task GetLatestCphSqliteUrl_WhenSqliteExists_Returns200WithPresignedUrl()
    {
        var objects = new List<StorageObjectInfo>
        {
            CreateStorageObject("views/cphs_20260101T120000Z.sqlite", 1024000),
            CreateStorageObject("views/cphs_20260102T120000Z.sqlite", 1048000)
        };

        _mockStorageService
            .Setup(s => s.ListAsync("views/", It.IsAny<CancellationToken>()))
            .ReturnsAsync(objects);

        _mockStorageService
            .Setup(s => s.GeneratePresignedUrl("views/cphs_20260102T120000Z.sqlite", It.IsAny<TimeSpan>()))
            .Returns("https://s3.amazonaws.com/bucket/views/cphs_20260102T120000Z.sqlite?signature=abc");

        var result = await _controller.GetLatestCphSqliteUrl(cancellationToken: CancellationToken.None);

        var okResult = result.Should().BeOfType<OkObjectResult>().Subject;
        okResult.StatusCode.Should().Be(StatusCodes.Status200OK);

        var response = okResult.Value.Should().BeOfType<CphSqliteLatestResponse>().Subject;
        response.ObjectKey.Should().Be("views/cphs_20260102T120000Z.sqlite");
        response.DownloadUrl.Should().Contain("signature=abc");
        response.Size.Should().Be(1048000);
    }

    [Fact]
    public async Task GetLatestCphSqliteUrl_WhenNoSqliteFiles_Returns404()
    {
        _mockStorageService
            .Setup(s => s.ListAsync("views/", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<StorageObjectInfo>());

        var result = await _controller.GetLatestCphSqliteUrl(cancellationToken: CancellationToken.None);

        var notFoundResult = result.Should().BeOfType<NotFoundObjectResult>().Subject;
        notFoundResult.StatusCode.Should().Be(StatusCodes.Status404NotFound);

        var response = notFoundResult.Value.Should().BeOfType<CphSqliteErrorResponse>().Subject;
        response.Message.Should().Contain("No CPH SQLite export files found");
    }

    [Fact]
    public async Task GetLatestCphSqliteUrl_SelectsLatestByKeyOrder()
    {
        var objects = new List<StorageObjectInfo>
        {
            CreateStorageObject("views/cphs_20260103T080000Z.sqlite", 500000),
            CreateStorageObject("views/cphs_20260101T120000Z.sqlite", 1024000),
            CreateStorageObject("views/cphs_20260102T120000Z.sqlite", 800000)
        };

        _mockStorageService
            .Setup(s => s.ListAsync("views/", It.IsAny<CancellationToken>()))
            .ReturnsAsync(objects);

        _mockStorageService
            .Setup(s => s.GeneratePresignedUrl(It.IsAny<string>(), It.IsAny<TimeSpan>()))
            .Returns("https://example.com/url");

        var result = await _controller.GetLatestCphSqliteUrl(cancellationToken: CancellationToken.None);

        var okResult = result.Should().BeOfType<OkObjectResult>().Subject;
        var response = okResult.Value.Should().BeOfType<CphSqliteLatestResponse>().Subject;
        response.ObjectKey.Should().Be("views/cphs_20260103T080000Z.sqlite");
    }

    [Fact]
    public async Task GetLatestCphSqliteUrl_IgnoresNonCphSqliteFiles()
    {
        var objects = new List<StorageObjectInfo>
        {
            CreateStorageObject("views/other_file.sqlite", 500000),
            CreateStorageObject("views/cphs_20260101T120000Z.sqlite", 1024000),
            CreateStorageObject("views/readme.txt", 100)
        };

        _mockStorageService
            .Setup(s => s.ListAsync("views/", It.IsAny<CancellationToken>()))
            .ReturnsAsync(objects);

        _mockStorageService
            .Setup(s => s.GeneratePresignedUrl(It.IsAny<string>(), It.IsAny<TimeSpan>()))
            .Returns("https://example.com/url");

        var result = await _controller.GetLatestCphSqliteUrl(cancellationToken: CancellationToken.None);

        var okResult = result.Should().BeOfType<OkObjectResult>().Subject;
        var response = okResult.Value.Should().BeOfType<CphSqliteLatestResponse>().Subject;
        response.ObjectKey.Should().Be("views/cphs_20260101T120000Z.sqlite");
    }

    [Fact]
    public async Task GetLatestCphSqliteUrl_WithCustomExpiry_UsesProvidedValue()
    {
        var objects = new List<StorageObjectInfo>
        {
            CreateStorageObject("views/cphs_20260101T120000Z.sqlite", 1024000)
        };

        _mockStorageService
            .Setup(s => s.ListAsync("views/", It.IsAny<CancellationToken>()))
            .ReturnsAsync(objects);

        _mockStorageService
            .Setup(s => s.GeneratePresignedUrl("views/cphs_20260101T120000Z.sqlite", TimeSpan.FromMinutes(30)))
            .Returns("https://example.com/url");

        var result = await _controller.GetLatestCphSqliteUrl(expiresInMinutes: 30, cancellationToken: CancellationToken.None);

        var okResult = result.Should().BeOfType<OkObjectResult>().Subject;
        var response = okResult.Value.Should().BeOfType<CphSqliteLatestResponse>().Subject;
        response.DownloadUrl.Should().Be("https://example.com/url");

        _mockStorageService.Verify(
            s => s.GeneratePresignedUrl("views/cphs_20260101T120000Z.sqlite", TimeSpan.FromMinutes(30)),
            Times.Once);
    }

    [Fact]
    public async Task GetLatestCphSqliteUrl_WhenServiceThrows_Returns500()
    {
        _mockStorageService
            .Setup(s => s.ListAsync("views/", It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("S3 unavailable"));

        var result = await _controller.GetLatestCphSqliteUrl(cancellationToken: CancellationToken.None);

        var statusResult = result.Should().BeOfType<ObjectResult>().Subject;
        statusResult.StatusCode.Should().Be(StatusCodes.Status500InternalServerError);

        var response = statusResult.Value.Should().BeOfType<CphSqliteErrorResponse>().Subject;
        response.Message.Should().Contain("unexpected error");
    }

    [Fact]
    public async Task GetLatestCphSqliteUrl_WhenCancelled_Returns499()
    {
        _mockStorageService
            .Setup(s => s.ListAsync("views/", It.IsAny<CancellationToken>()))
            .ThrowsAsync(new OperationCanceledException());

        var result = await _controller.GetLatestCphSqliteUrl(cancellationToken: CancellationToken.None);

        var statusResult = result.Should().BeOfType<ObjectResult>().Subject;
        statusResult.StatusCode.Should().Be(499);
    }

    [Fact]
    public async Task GetLatestCphSqliteUrl_ResponseContainsExpiresAt()
    {
        var objects = new List<StorageObjectInfo>
        {
            CreateStorageObject("views/cphs_20260101T120000Z.sqlite", 1024000)
        };

        _mockStorageService
            .Setup(s => s.ListAsync("views/", It.IsAny<CancellationToken>()))
            .ReturnsAsync(objects);

        _mockStorageService
            .Setup(s => s.GeneratePresignedUrl(It.IsAny<string>(), It.IsAny<TimeSpan>()))
            .Returns("https://example.com/url");

        var beforeCall = DateTime.UtcNow;
        var result = await _controller.GetLatestCphSqliteUrl(cancellationToken: CancellationToken.None);

        var okResult = result.Should().BeOfType<OkObjectResult>().Subject;
        var response = okResult.Value.Should().BeOfType<CphSqliteLatestResponse>().Subject;
        response.ExpiresAt.Should().BeAfter(beforeCall);
        response.ExpiresAt.Should().BeCloseTo(DateTime.UtcNow.AddHours(1), TimeSpan.FromSeconds(10));
    }

    private static StorageObjectInfo CreateStorageObject(string key, long size)
    {
        return new StorageObjectInfo
        {
            Container = "test-bucket",
            Key = key,
            Size = size,
            LastModified = DateTimeOffset.UtcNow,
            StorageUri = new Uri($"s3://test-bucket/{key}")
        };
    }
}
