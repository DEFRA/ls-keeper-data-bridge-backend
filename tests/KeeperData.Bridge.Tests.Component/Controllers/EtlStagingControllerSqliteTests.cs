using FluentAssertions;
using KeeperData.Bridge.Controllers;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Moq;

namespace KeeperData.Bridge.Tests.Component.Controllers;

/// <summary>The SQLite read-model download endpoint. The DuckDB route beside it is unchanged, so only
/// the behaviour specific to views/ is covered here.</summary>
public class EtlStagingControllerSqliteTests
{
    private readonly Mock<IEtlPipelineStorageProvider> _storageProvider = new();
    private readonly Mock<IBlobStorageService> _views = new();
    private readonly EtlStagingController _controller;

    public EtlStagingControllerSqliteTests()
    {
        _storageProvider.Setup(p => p.ForFolder(EtlPipelineFolders.Views)).Returns(_views.Object);

        _controller = new EtlStagingController(
            _storageProvider.Object,
            Mock.Of<ILogger<EtlStagingController>>());
    }

    private void Listing(params StorageObjectInfo[] objects)
        => _views.Setup(s => s.ListAsync(string.Empty, It.IsAny<CancellationToken>()))
            .ReturnsAsync(objects);

    private void Presigns(string url = "https://example.test/download?signature=abc")
        => _views.Setup(s => s.GeneratePresignedUrl(It.IsAny<string>(), It.IsAny<TimeSpan>())).Returns(url);

    private static StorageObjectInfo Object(string key, long size = 1024) => new()
    {
        Container = "internal",
        Key = key,
        Size = size,
        LastModified = new DateTimeOffset(2026, 8, 21, 7, 0, 3, TimeSpan.Zero),
        StorageUri = new Uri($"s3://internal/{key}")
    };

    [Fact]
    public async Task Returns_a_presigned_url_for_the_newest_read_model()
    {
        Listing(Object("krds-db_20260820070003.sqlite"), Object("krds-db_20260821070003.sqlite", 2048));
        Presigns();

        var result = await _controller.GetLatestSqliteUrl();

        var response = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<StagingDatabaseLatestResponse>().Subject;

        response.ObjectKey.Should().Be("views/krds-db_20260821070003.sqlite");
        response.DownloadUrl.Should().Contain("signature=abc");
        response.Size.Should().Be(2048);
    }

    [Fact]
    public async Task Ignores_the_legacy_cph_export_sharing_the_folder()
    {
        Listing(Object("cphs_20260822T120000Z.sqlite"), Object("krds-db_20260821070003.sqlite"));
        Presigns();

        var result = await _controller.GetLatestSqliteUrl();

        result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<StagingDatabaseLatestResponse>()
            .Which.ObjectKey.Should().Be("views/krds-db_20260821070003.sqlite",
                "cphs_ sorts later but belongs to a different producer");
    }

    [Fact]
    public async Task Returns_404_when_the_pipeline_has_not_produced_one()
    {
        Listing();

        var result = await _controller.GetLatestSqliteUrl();

        result.Should().BeOfType<NotFoundObjectResult>()
            .Which.Value.Should().BeOfType<StagingDatabaseErrorResponse>()
            .Which.Message.Should().Contain("No SQLite read model found");
    }

    [Fact]
    public async Task Caps_the_link_lifetime_because_the_read_model_carries_personal_data()
    {
        Listing(Object("krds-db_20260821070003.sqlite"));
        Presigns();

        await _controller.GetLatestSqliteUrl(expiresInMinutes: 10_080);

        _views.Verify(s => s.GeneratePresignedUrl(It.IsAny<string>(), TimeSpan.FromMinutes(60)), Times.Once);
    }

    [Fact]
    public async Task Honours_a_shorter_requested_lifetime()
    {
        Listing(Object("krds-db_20260821070003.sqlite"));
        Presigns();

        await _controller.GetLatestSqliteUrl(expiresInMinutes: 5);

        _views.Verify(s => s.GeneratePresignedUrl(It.IsAny<string>(), TimeSpan.FromMinutes(5)), Times.Once);
    }

    [Fact]
    public async Task Returns_500_when_storage_fails()
    {
        _views.Setup(s => s.ListAsync(string.Empty, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("bucket unreachable"));

        var result = await _controller.GetLatestSqliteUrl();

        var status = result.Should().BeOfType<ObjectResult>().Subject;
        status.StatusCode.Should().Be(StatusCodes.Status500InternalServerError);
        status.Value.Should().BeOfType<StagingDatabaseErrorResponse>()
            .Which.Message.Should().NotContain("bucket unreachable", "internal detail stays in the log");
    }
}
