using FluentAssertions;
using KeeperData.Bridge.Controllers;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Moq;

namespace KeeperData.Bridge.Tests.Component.Controllers;

/// <summary>The per-dataset parquet snapshot download endpoint. Ordering comes from the timestamp
/// encoded in the snapshot's name, as it does for the pipeline, never from object modified time.</summary>
public class EtlStagingControllerSnapshotsTests
{
    private static readonly DataSetDefinition SamCphHoldings = new(
        "sam_cph_holdings",
        "LITP_SAMCPHHOLDING_",
        ["cph"],
        "change_type",
        [],
        Format: FileFormat.Hcdt,
        IngestionMode: DataSetIngestionMode.Delta);

    private readonly Mock<IEtlPipelineStorageProvider> _storageProvider = new();
    private readonly Mock<IBlobStorageService> _snapshots = new();
    private readonly Mock<IDataSetDefinitions> _dataSetDefinitions = new();
    private readonly EtlStagingController _controller;

    public EtlStagingControllerSnapshotsTests()
    {
        _storageProvider.Setup(p => p.ForFolder(EtlPipelineFolders.Snapshots)).Returns(_snapshots.Object);
        _dataSetDefinitions.SetupGet(d => d.All).Returns([SamCphHoldings]);

        _controller = new EtlStagingController(
            _storageProvider.Object,
            _dataSetDefinitions.Object,
            Mock.Of<ILogger<EtlStagingController>>());
    }

    private void Listing(params StorageObjectInfo[] objects)
        => _snapshots.Setup(s => s.ListAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(objects);

    private void Presigns(string url = "https://example.test/download?signature=abc")
        => _snapshots.Setup(s => s.GeneratePresignedUrl(It.IsAny<string>(), It.IsAny<TimeSpan>())).Returns(url);

    private static StorageObjectInfo Object(string key, long size = 1024,
        DateTimeOffset? lastModified = null) => new()
    {
        Container = "internal",
        Key = key,
        Size = size,
        LastModified = lastModified ?? new DateTimeOffset(2026, 8, 21, 7, 0, 3, TimeSpan.Zero),
        StorageUri = new Uri($"s3://internal/{key}")
    };

    [Fact]
    public async Task Returns_a_presigned_url_for_the_newest_snapshot()
    {
        Listing(
            Object("sam_cph_holdings/sam_cph_holdings_20260821070003.parquet", 2048),
            Object("sam_cph_holdings/sam_cph_holdings_20260820070003.parquet"));
        Presigns();

        var result = await _controller.GetLatestSnapshotUrl("sam_cph_holdings");

        var response = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<StagingDatabaseLatestResponse>().Subject;

        response.ObjectKey.Should().Be("snapshots/sam_cph_holdings/sam_cph_holdings_20260821070003.parquet");
        response.DownloadUrl.Should().Contain("signature=abc");
        response.Size.Should().Be(2048);
    }

    [Fact]
    public async Task Picks_by_the_timestamp_in_the_name_not_the_modified_time()
    {
        Listing(
            Object("sam_cph_holdings/sam_cph_holdings_20260820070003.parquet",
                lastModified: new DateTimeOffset(2026, 8, 22, 0, 0, 0, TimeSpan.Zero)),
            Object("sam_cph_holdings/sam_cph_holdings_20260821070003.parquet",
                lastModified: new DateTimeOffset(2026, 8, 20, 0, 0, 0, TimeSpan.Zero)));
        Presigns();

        var result = await _controller.GetLatestSnapshotUrl("sam_cph_holdings");

        result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<StagingDatabaseLatestResponse>()
            .Which.ObjectKey.Should().Be("snapshots/sam_cph_holdings/sam_cph_holdings_20260821070003.parquet",
                "a re-uploaded older snapshot must not shadow the newer one");
    }

    [Fact]
    public async Task Reads_the_timestamp_past_a_baseline_hash_segment()
    {
        Listing(
            Object("sam_cph_holdings/sam_cph_holdings_20260821070003.parquet"),
            Object("sam_cph_holdings/sam_cph_holdings_b1a2b3c4_20260822070003.parquet"));
        Presigns();

        var result = await _controller.GetLatestSnapshotUrl("sam_cph_holdings");

        result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<StagingDatabaseLatestResponse>()
            .Which.ObjectKey.Should().Be("snapshots/sam_cph_holdings/sam_cph_holdings_b1a2b3c4_20260822070003.parquet");
    }

    [Fact]
    public async Task Matches_the_dataset_name_case_insensitively()
    {
        Listing(Object("sam_cph_holdings/sam_cph_holdings_20260821070003.parquet"));
        Presigns();

        var result = await _controller.GetLatestSnapshotUrl("SAM_CPH_HOLDINGS");

        result.Should().BeOfType<OkObjectResult>();
    }

    [Fact]
    public async Task Returns_400_for_a_dataset_the_pipeline_does_not_know()
    {
        var result = await _controller.GetLatestSnapshotUrl("not_a_dataset");

        result.Should().BeOfType<BadRequestObjectResult>()
            .Which.Value.Should().BeOfType<StagingDatabaseErrorResponse>()
            .Which.Message.Should().Contain("not_a_dataset");

        _snapshots.Verify(s => s.ListAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Returns_404_when_the_dataset_has_no_snapshot()
    {
        Listing();

        var result = await _controller.GetLatestSnapshotUrl("sam_cph_holdings");

        result.Should().BeOfType<NotFoundObjectResult>()
            .Which.Value.Should().BeOfType<StagingDatabaseErrorResponse>()
            .Which.Message.Should().Contain("sam_cph_holdings");
    }

    [Fact]
    public async Task Caps_the_link_lifetime_at_seven_days_like_the_staging_database()
    {
        Listing(Object("sam_cph_holdings/sam_cph_holdings_20260821070003.parquet"));
        Presigns();

        await _controller.GetLatestSnapshotUrl("sam_cph_holdings", expiresInMinutes: 20_000);

        _snapshots.Verify(s => s.GeneratePresignedUrl(
            It.IsAny<string>(), TimeSpan.FromMinutes(10_080)), Times.Once);
    }

    [Fact]
    public async Task Returns_500_when_storage_fails()
    {
        _snapshots.Setup(s => s.ListAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("bucket unreachable"));

        var result = await _controller.GetLatestSnapshotUrl("sam_cph_holdings");

        var status = result.Should().BeOfType<ObjectResult>().Subject;
        status.StatusCode.Should().Be(StatusCodes.Status500InternalServerError);
        status.Value.Should().BeOfType<StagingDatabaseErrorResponse>()
            .Which.Message.Should().NotContain("bucket unreachable", "internal detail stays in the log");
    }
}
