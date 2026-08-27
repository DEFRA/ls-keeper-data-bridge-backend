using FluentAssertions;
using KeeperData.Bridge.Controllers;
using KeeperData.Bridge.Models;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Moq;

namespace KeeperData.Bridge.Tests.Component.Controllers;

public class EtlStorageControllerTests
{
    private readonly Mock<IBlobStorageServiceFactory> _blobFactory = new();
    private readonly Mock<IEtlPipelineStorageProvider> _storageProvider = new();
    private readonly Mock<IBlobStorageService> _inbound = new();
    private readonly Mock<IBlobStorageService> _qaSource = new();
    private readonly Mock<IBlobStorageService> _raw = new();
    private readonly Mock<IBlobStorageService> _normalised = new();
    private readonly Mock<IBlobStorageService> _snapshots = new();
    private readonly Mock<IBlobStorageService> _staging = new();
    private readonly Mock<IWebHostEnvironment> _environment = new();

    public EtlStorageControllerTests()
    {
        _environment.SetupGet(e => e.EnvironmentName).Returns("Development");
        _blobFactory.Setup(f => f.Get()).Returns(_inbound.Object);
        _blobFactory.Setup(f => f.GetSourceInternal()).Returns(_qaSource.Object);
        _storageProvider.Setup(p => p.ForFolder(EtlPipelineFolders.Raw)).Returns(_raw.Object);
        _storageProvider.Setup(p => p.ForFolder(EtlPipelineFolders.Normalised)).Returns(_normalised.Object);
        _storageProvider.Setup(p => p.ForFolder(EtlPipelineFolders.Snapshots)).Returns(_snapshots.Object);
        _storageProvider.Setup(p => p.ForFolder(EtlPipelineFolders.Staging)).Returns(_staging.Object);
    }

    [Fact]
    public async Task Targeted_normalised_purge_only_deletes_the_dataset_folder()
    {
        Page(_normalised, "sam_cph_holdings/",
            Object("sam_cph_holdings/LITP_SAMCPHHOLDING_20260819203115.parquet"));

        var result = await Controller().PurgeStorage("sam_cph_holdings", "normalised");

        var response = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStoragePurgeResponse>().Subject;
        response.DeletedCount.Should().Be(1);
        response.DeletedKeys.Should().Equal(
            "normalised/sam_cph_holdings/LITP_SAMCPHHOLDING_20260819203115.parquet");

        _normalised.Verify(s => s.DeleteByPrefixAsync(
            "sam_cph_holdings/", It.IsAny<CancellationToken>()), Times.Once);
        _raw.Verify(s => s.DeleteByPrefixAsync(
            It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        _snapshots.Verify(s => s.DeleteByPrefixAsync(
            It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Targeted_all_stage_purge_scopes_each_dataset_stage_and_leaves_shared_staging_untouched()
    {
        const string filePrefix = "LITP_SAMCPHHOLDING_";
        const string datasetPrefix = "sam_cph_holdings/";

        Page(_inbound, filePrefix, Object("LITP_SAMCPHHOLDING_20260819203115.csv"));
        Page(_raw, filePrefix, Object("LITP_SAMCPHHOLDING_20260819203115.psv"));
        Page(_normalised, datasetPrefix, Object("sam_cph_holdings/a.parquet"));
        Page(_snapshots, datasetPrefix, Object("sam_cph_holdings/b.parquet"));

        var result = await Controller().PurgeStorage("sam_cph_holdings", "all");

        result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStoragePurgeResponse>()
            .Which.DeletedKeys.Should().BeEquivalentTo([
                "dest/LITP_SAMCPHHOLDING_20260819203115.csv",
                "raw/LITP_SAMCPHHOLDING_20260819203115.psv",
                "normalised/sam_cph_holdings/a.parquet",
                "snapshots/sam_cph_holdings/b.parquet"
            ]);

        _staging.Verify(s => s.DeleteByPrefixAsync(
            It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task All_datasets_staging_purge_deletes_the_complete_shared_folder()
    {
        Page(_staging, null,
            Object("krds-db_20260820070003.duckdb"),
            Object("krds-db_20260821070003.duckdb"));

        var result = await Controller().PurgeStorage("all", "staging");

        var response = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStoragePurgeResponse>().Subject;
        response.DeletedCount.Should().Be(2);
        response.DeletedKeys.Should().OnlyContain(key => key.StartsWith("staging/"));
        _staging.Verify(s => s.DeleteByPrefixAsync(
            string.Empty, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Explicit_all_datasets_and_all_stages_provides_a_complete_clean_slate()
    {
        Page(_inbound, null, Object("inbound.csv"));
        Page(_raw, null, Object("raw.psv"));
        Page(_normalised, null, Object("sam_cph_holdings/a.parquet"));
        Page(_snapshots, null, Object("sam_cph_holdings/b.parquet"));
        Page(_staging, null, Object("krds-db.duckdb"));

        var result = await Controller().PurgeStorage("all", "all");

        result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStoragePurgeResponse>()
            .Which.DeletedKeys.Should().BeEquivalentTo([
                "dest/inbound.csv",
                "raw/raw.psv",
                "normalised/sam_cph_holdings/a.parquet",
                "snapshots/sam_cph_holdings/b.parquet",
                "staging/krds-db.duckdb"
            ]);
    }

    [Theory]
    [InlineData(null, "all")]
    [InlineData("all", null)]
    [InlineData("", "all")]
    [InlineData("all", " ")]
    public async Task Dataset_and_stage_must_be_supplied_explicitly(string? dataset, string? stage)
    {
        var result = await Controller().PurgeStorage(dataset, stage);

        result.Should().BeOfType<BadRequestObjectResult>()
            .Which.Value.Should().BeOfType<ErrorResponse>()
            .Which.Message.Should().Contain("dataset and stage query parameters are required");
        _blobFactory.VerifyNoOtherCalls();
        _storageProvider.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task Dataset_scoped_staging_purge_is_rejected_because_staging_is_shared()
    {
        var result = await Controller().PurgeStorage("sam_cph_holdings", "staging");

        result.Should().BeOfType<BadRequestObjectResult>()
            .Which.Value.Should().BeOfType<ErrorResponse>()
            .Which.Message.Should().Contain("shared all-dataset database");
        _staging.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task External_inbound_purge_uses_the_writable_QA_source_folder()
    {
        Page(_qaSource, "LITP_SAMCPHHOLDING_", Object("LITP_SAMCPHHOLDING_20260819203115.csv"));

        var result = await Controller().PurgeStorage("sam_cph_holdings", "inbound", "external");

        result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStoragePurgeResponse>()
            .Which.DeletedKeys.Should().Equal("qasrc/LITP_SAMCPHHOLDING_20260819203115.csv");
        _blobFactory.Verify(f => f.GetSourceInternal(), Times.Once);
        _blobFactory.Verify(f => f.Get(), Times.Never);
    }

    [Fact]
    public async Task Production_rejects_the_request_before_accessing_storage()
    {
        _environment.SetupGet(e => e.EnvironmentName).Returns("Production");

        var result = await Controller().PurgeStorage("all", "all");

        var forbidden = result.Should().BeOfType<ObjectResult>().Subject;
        forbidden.StatusCode.Should().Be(StatusCodes.Status403Forbidden);
        forbidden.Value.Should().BeOfType<ErrorResponse>()
            .Which.Message.Should().Be("Storage purge endpoint is disabled in production environments.");
        _blobFactory.VerifyNoOtherCalls();
        _storageProvider.VerifyNoOtherCalls();
    }

    [Theory]
    [InlineData("unknown", "normalised", "internal", "not recognized")]
    [InlineData("all", "other", "internal", "Invalid stage")]
    [InlineData("all", "raw", "internet", "Invalid sourceType")]
    public async Task Invalid_parameters_return_400(
        string dataset,
        string stage,
        string sourceType,
        string expectedMessage)
    {
        var result = await Controller().PurgeStorage(dataset, stage, sourceType);

        result.Should().BeOfType<BadRequestObjectResult>()
            .Which.Value.Should().BeOfType<ErrorResponse>()
            .Which.Message.Should().Contain(expectedMessage);
    }

    [Fact]
    public async Task Storage_cancellation_returns_499()
    {
        _raw.Setup(s => s.DeleteByPrefixAsync(
                It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new OperationCanceledException());

        var result = await Controller().PurgeStorage("all", "raw");

        var cancelled = result.Should().BeOfType<ObjectResult>().Subject;
        cancelled.StatusCode.Should().Be(StatusCodes.Status499ClientClosedRequest);
    }

    [Fact]
    public async Task Storage_failure_returns_a_safe_500_message()
    {
        _raw.Setup(s => s.DeleteByPrefixAsync(
                It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("bucket credentials leaked here"));

        var result = await Controller().PurgeStorage("all", "raw");

        var failed = result.Should().BeOfType<ObjectResult>().Subject;
        failed.StatusCode.Should().Be(StatusCodes.Status500InternalServerError);
        failed.Value.Should().BeOfType<ErrorResponse>()
            .Which.Message.Should().NotContain("credentials");
    }

    private EtlStorageController Controller()
    {
        var definitions = new Mock<IDataSetDefinitions>();
        definitions.SetupGet(d => d.All).Returns(StandardDataSetDefinitionsBuilder.Build().All);

        return new EtlStorageController(
            _blobFactory.Object,
            _storageProvider.Object,
            definitions.Object,
            _environment.Object,
            TimeProvider.System,
            Mock.Of<ILogger<EtlStorageController>>());
    }

    private static void Page(
        Mock<IBlobStorageService> storage,
        string? prefix,
        params StorageObjectInfo[] objects)
        => storage.Setup(s => s.DeleteByPrefixAsync(
                prefix ?? string.Empty, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ClearDownResult
            {
                DeletedKeys = objects.Select(item => item.Key).ToArray(),
                TotalDeleted = objects.Length
            });

    private static StorageObjectInfo Object(string key) => new()
    {
        Container = "internal",
        Key = key,
        StorageUri = new Uri($"s3://internal/{key}")
    };
}
