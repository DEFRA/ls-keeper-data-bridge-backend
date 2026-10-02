using FluentAssertions;
using KeeperData.Bridge.Config;
using KeeperData.Bridge.Controllers;
using KeeperData.Bridge.Models;
using KeeperData.Core.ETL.Abstract;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Storage;
using KeeperData.Core.Storage.Dtos;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
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
    private readonly Mock<IBlobStorageService> _optimised = new();
    private readonly Mock<IBlobStorageService> _snapshots = new();
    private readonly Mock<IBlobStorageService> _staging = new();
    private readonly Mock<IBlobStorageService> _views = new();
    private readonly Mock<IEtlImportStatusStore> _statusStore = new();
    private readonly Mock<IWebHostEnvironment> _environment = new();
    private bool _etlStoragePurgeEnabled;

    public EtlStorageControllerTests()
    {
        _environment.SetupGet(e => e.EnvironmentName).Returns("Development");
        _blobFactory.Setup(f => f.Get()).Returns(_inbound.Object);
        _blobFactory.Setup(f => f.GetSourceInternal()).Returns(_qaSource.Object);
        _storageProvider.Setup(p => p.ForFolder(EtlPipelineFolders.Raw)).Returns(_raw.Object);
        _storageProvider.Setup(p => p.ForFolder(EtlPipelineFolders.Normalised)).Returns(_normalised.Object);
        _storageProvider.Setup(p => p.ForFolder(EtlPipelineFolders.Optimised)).Returns(_optimised.Object);
        _storageProvider.Setup(p => p.ForFolder(EtlPipelineFolders.Snapshots)).Returns(_snapshots.Object);
        _storageProvider.Setup(p => p.ForFolder(EtlPipelineFolders.Staging)).Returns(_staging.Object);
        _storageProvider.Setup(p => p.ForFolder(EtlPipelineFolders.Views)).Returns(_views.Object);
        _statusStore.Setup(s => s.GetInFlightAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync((EtlImportDocument?)null);

        // A stage the test does not page anything into still gets purged by the cascade - the
        // folders it reaches are empty rather than untouched.
        foreach (var storage in new[] { _inbound, _qaSource, _raw, _normalised, _optimised, _snapshots, _staging, _views })
        {
            storage.Setup(s => s.DeleteByPrefixAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ClearDownResult { DeletedKeys = [], TotalDeleted = 0 });
            storage.Setup(s => s.EnumerateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns(Streamed([]));
        }
    }

    [Fact]
    public async Task Targeted_normalised_purge_cascades_to_everything_derived_from_it()
    {
        Page(_normalised, "sam_cph_holdings/",
            Object("sam_cph_holdings/LITP_SAMCPHHOLDING_20260819203115.parquet"));
        Page(_optimised, "sam_cph_holdings/", Object("sam_cph_holdings/o.parquet"));
        Page(_snapshots, "sam_cph_holdings/", Object("sam_cph_holdings/s.parquet"));
        Page(_staging, null, Object("krds-db.duckdb"));
        Page(_views, null, Object("krds-db.sqlite"));

        var result = await Controller().PurgeStorage("sam_cph_holdings", "normalised");

        var response = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStoragePurgeResponse>().Subject;
        response.PurgedStages.Should().Equal("normalised", "optimised", "snapshots", "staging", "views");
        response.DeletedKeys.Should().BeEquivalentTo([
            "normalised/sam_cph_holdings/LITP_SAMCPHHOLDING_20260819203115.parquet",
            "optimised/sam_cph_holdings/o.parquet",
            "snapshots/sam_cph_holdings/s.parquet",
            "staging/krds-db.duckdb",
            "views/krds-db.sqlite"
        ]);

        _normalised.Verify(s => s.DeleteByPrefixAsync(
            "sam_cph_holdings/", It.IsAny<CancellationToken>()), Times.Once);
        _raw.Verify(s => s.DeleteByPrefixAsync(
            It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Targeted_snapshots_purge_clears_the_shared_derived_folders()
    {
        Page(_snapshots, "sam_cph_holdings/", Object("sam_cph_holdings/s.parquet"));
        Page(_staging, null, Object("krds-db.duckdb"));
        Page(_views, null, Object("krds-db.sqlite"));

        var result = await Controller().PurgeStorage("sam_cph_holdings", "snapshots");

        var response = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStoragePurgeResponse>().Subject;
        response.PurgedStages.Should().Equal("snapshots", "staging", "views");
        response.DeletedKeys.Should().BeEquivalentTo([
            "snapshots/sam_cph_holdings/s.parquet",
            "staging/krds-db.duckdb",
            "views/krds-db.sqlite"
        ]);

        _snapshots.Verify(s => s.DeleteByPrefixAsync(
            "sam_cph_holdings/", It.IsAny<CancellationToken>()), Times.Once);
        _staging.Verify(s => s.DeleteByPrefixAsync(
            string.Empty, It.IsAny<CancellationToken>()), Times.Once);
        _views.Verify(s => s.DeleteByPrefixAsync(
            string.Empty, It.IsAny<CancellationToken>()), Times.Once);
        _normalised.Verify(s => s.DeleteByPrefixAsync(
            It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        _optimised.Verify(s => s.DeleteByPrefixAsync(
            It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Targeted_optimised_purge_deletes_the_dataset_folder_and_everything_downstream()
    {
        Page(_optimised, "sam_cph_holdings/", Object("sam_cph_holdings/o.parquet"));

        var result = await Controller().PurgeStorage("sam_cph_holdings", "optimised");

        var response = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStoragePurgeResponse>().Subject;
        response.PurgedStages.Should().Equal("optimised", "snapshots", "staging", "views");
        response.DeletedKeys.Should().Equal("optimised/sam_cph_holdings/o.parquet");

        _optimised.Verify(s => s.DeleteByPrefixAsync(
            "sam_cph_holdings/", It.IsAny<CancellationToken>()), Times.Once);
        _normalised.Verify(s => s.DeleteByPrefixAsync(
            It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Targeted_all_stage_purge_scopes_each_dataset_stage_and_clears_the_shared_derived_folders()
    {
        const string filePrefix = "litprd/LITP_SAMCPHHOLDING_";
        const string datasetPrefix = "sam_cph_holdings/";

        Page(_inbound, filePrefix, Object("litprd/LITP_SAMCPHHOLDING_20260819203115.csv"));
        Page(_raw, filePrefix, Object("litprd/LITP_SAMCPHHOLDING_20260819203115.psv"));
        Page(_normalised, datasetPrefix, Object("sam_cph_holdings/a.parquet"));
        Page(_optimised, datasetPrefix, Object("sam_cph_holdings/a2.parquet"));
        Page(_snapshots, datasetPrefix, Object("sam_cph_holdings/b.parquet"));
        Page(_staging, null, Object("krds-db.duckdb"));
        Page(_views, null, Object("krds-db.sqlite"));

        var result = await Controller().PurgeStorage("sam_cph_holdings", "all");

        result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStoragePurgeResponse>()
            .Which.DeletedKeys.Should().BeEquivalentTo([
                "dest/litprd/LITP_SAMCPHHOLDING_20260819203115.csv",
                "raw/litprd/LITP_SAMCPHHOLDING_20260819203115.psv",
                "normalised/sam_cph_holdings/a.parquet",
                "optimised/sam_cph_holdings/a2.parquet",
                "snapshots/sam_cph_holdings/b.parquet",
                "staging/krds-db.duckdb",
                "views/krds-db.sqlite"
            ]);
    }

    [Fact]
    public async Task All_datasets_staging_purge_deletes_the_shared_folder_and_the_view_built_from_it()
    {
        Page(_staging, null,
            Object("krds-db_20260820070003.duckdb"),
            Object("krds-db_20260821070003.duckdb"));
        Page(_views, null, Object("krds-db_20260821070003.sqlite"));

        var result = await Controller().PurgeStorage("all", "staging");

        var response = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStoragePurgeResponse>().Subject;
        response.PurgedStages.Should().Equal("staging", "views");
        response.DeletedCount.Should().Be(3);
        _staging.Verify(s => s.DeleteByPrefixAsync(
            string.Empty, It.IsAny<CancellationToken>()), Times.Once);
        _views.Verify(s => s.DeleteByPrefixAsync(
            string.Empty, It.IsAny<CancellationToken>()), Times.Once);
        _snapshots.Verify(s => s.DeleteByPrefixAsync(
            It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task All_datasets_views_purge_deletes_only_the_view_folder()
    {
        Page(_views, null, Object("krds-db.sqlite"));

        var result = await Controller().PurgeStorage("all", "views");

        var response = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStoragePurgeResponse>().Subject;
        response.PurgedStages.Should().Equal("views");
        response.DeletedKeys.Should().Equal("views/krds-db.sqlite");
        _staging.VerifyNoOtherCalls();
        _snapshots.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task Explicit_all_datasets_and_all_stages_provides_a_complete_clean_slate()
    {
        Page(_inbound, null, Object("inbound.csv"));
        Page(_raw, null, Object("raw.psv"));
        Page(_normalised, null, Object("sam_cph_holdings/a.parquet"));
        Page(_optimised, null, Object("sam_cph_holdings/a2.parquet"));
        Page(_snapshots, null, Object("sam_cph_holdings/b.parquet"));
        Page(_staging, null, Object("krds-db.duckdb"));
        Page(_views, null, Object("krds-db.sqlite"));

        var result = await Controller().PurgeStorage("all", "all");

        result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStoragePurgeResponse>()
            .Which.DeletedKeys.Should().BeEquivalentTo([
                "dest/inbound.csv",
                "raw/raw.psv",
                "normalised/sam_cph_holdings/a.parquet",
                "optimised/sam_cph_holdings/a2.parquet",
                "snapshots/sam_cph_holdings/b.parquet",
                "staging/krds-db.duckdb",
                "views/krds-db.sqlite"
            ]);
    }

    [Fact]
    public async Task Successful_purge_is_recorded_in_import_history()
    {
        Page(_snapshots, "sam_cph_holdings/", Object("sam_cph_holdings/s.parquet"));

        var result = await Controller().PurgeStorage("sam_cph_holdings", "snapshots");

        var response = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStoragePurgeResponse>().Subject;
        response.PurgeId.Should().NotBeEmpty();
        _statusStore.Verify(s => s.RecordPurgeAsync(
            It.Is<EtlPurgeRecord>(p =>
                p.PurgeId == response.PurgeId &&
                p.Dataset == "sam_cph_holdings" &&
                p.Stages.SequenceEqual(new[] { "snapshots", "staging", "views" }) &&
                p.DeletedCount == response.DeletedCount),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Failed_purge_records_nothing_in_import_history()
    {
        _raw.Setup(s => s.DeleteByPrefixAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("storage went away"));

        var result = await Controller().PurgeStorage("all", "raw");

        result.Should().BeOfType<ObjectResult>()
            .Which.StatusCode.Should().Be(StatusCodes.Status500InternalServerError);
        _statusStore.Verify(s => s.RecordPurgeAsync(
            It.IsAny<EtlPurgeRecord>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Purge_is_rejected_while_an_import_is_in_flight()
    {
        var inFlight = new EtlImportDocument
        {
            ImportId = Guid.NewGuid(),
            Status = nameof(EtlImportStatus.Running),
            SourceType = "external"
        };
        _statusStore.Setup(s => s.GetInFlightAsync(It.IsAny<CancellationToken>())).ReturnsAsync(inFlight);

        var result = await Controller().PurgeStorage("all", "all");

        result.Should().BeOfType<ConflictObjectResult>()
            .Which.Value.Should().BeOfType<EtlImportConflictResponse>()
            .Which.InFlightImportId.Should().Be(inFlight.ImportId);
        _blobFactory.VerifyNoOtherCalls();
        _storageProvider.VerifyNoOtherCalls();
        _statusStore.Verify(s => s.RecordPurgeAsync(
            It.IsAny<EtlPurgeRecord>(), It.IsAny<CancellationToken>()), Times.Never);
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

    [Theory]
    [InlineData("staging")]
    [InlineData("views")]
    public async Task Dataset_scoped_shared_folder_purge_is_rejected_because_the_folder_is_shared(string stage)
    {
        var result = await Controller().PurgeStorage("sam_cph_holdings", stage);

        result.Should().BeOfType<BadRequestObjectResult>()
            .Which.Value.Should().BeOfType<ErrorResponse>()
            .Which.Message.Should().Contain("shared across every dataset");
        _staging.VerifyNoOtherCalls();
        _views.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task External_inbound_purge_uses_the_writable_QA_source_folder()
    {
        Page(_qaSource, "litprd/LITP_SAMCPHHOLDING_", Object("litprd/LITP_SAMCPHHOLDING_20260819203115.csv"));

        var result = await Controller().PurgeStorage("sam_cph_holdings", "inbound", "external");

        result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStoragePurgeResponse>()
            .Which.DeletedKeys.Should().Equal("qasrc/litprd/LITP_SAMCPHHOLDING_20260819203115.csv");
        _blobFactory.Verify(f => f.GetSourceInternal(), Times.Once);
        _blobFactory.Verify(f => f.Get(), Times.Never);
    }

    [Fact]
    public async Task Cts_inbound_purge_deletes_its_own_files_from_both_lanes_and_leaves_sibling_tables()
    {
        Lane(_qaSource, "cads/cts/bulk/",
            "cads/cts/bulk/CTSM_CADS_PREP_BULK_00001_001_CT_LOCATIONS_2026-07-28-094638.csv",
            "cads/cts/bulk/CTSM_CADS_PREP_BULK_00001_001_CT_LOCATION_PARTY_RELS_2026-07-28-094630.csv");
        Lane(_qaSource, "cads/cts/daily/",
            "cads/cts/daily/CTSM_CADS_PREP_DELTA_00002_001_CT_LOCATIONS_2026-07-30-141209.csv",
            "cads/cts/daily/CTSM_CADS_PREP_DELTA_00002_001_CT_LOCATION_IDENTIFIERS_2026-07-30-141210.csv");

        var result = await Controller().PurgeStorage("cts_locations", "inbound", "external");

        result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStoragePurgeResponse>()
            .Which.DeletedKeys.Should().Equal(
                "qasrc/cads/cts/bulk/CTSM_CADS_PREP_BULK_00001_001_CT_LOCATIONS_2026-07-28-094638.csv",
                "qasrc/cads/cts/daily/CTSM_CADS_PREP_DELTA_00002_001_CT_LOCATIONS_2026-07-30-141209.csv");

        _qaSource.Verify(s => s.DeleteByPrefixAsync(
            It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        _qaSource.Verify(s => s.DeleteAsync(
            It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact]
    public async Task Cts_raw_purge_matches_the_keys_the_source_folders_are_kept_under()
    {
        Lane(_raw, "cads/cts/bulk/",
            "cads/cts/bulk/CTSM_CADS_PREP_BULK_00001_001_CT_COUNTIES_2026-07-28-094644.psv");
        Lane(_raw, "cads/cts/daily/");

        var result = await Controller().PurgeStorage("cts_counties", "raw");

        result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStoragePurgeResponse>()
            .Which.DeletedKeys.Should().Equal(
                "raw/cads/cts/bulk/CTSM_CADS_PREP_BULK_00001_001_CT_COUNTIES_2026-07-28-094644.psv");
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

    [Fact]
    public async Task Production_hosted_ephemeral_environment_can_explicitly_enable_the_endpoint()
    {
        _environment.SetupGet(e => e.EnvironmentName).Returns("Production");
        _etlStoragePurgeEnabled = true;
        Page(_raw, null, Object("raw.psv"));

        var result = await Controller().PurgeStorage("all", "raw");

        result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStoragePurgeResponse>()
            .Which.DeletedKeys.Should().Equal("raw/raw.psv");
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

    [Fact]
    public async Task Report_lists_a_stage_folder_grouped_by_dataset()
    {
        Lane(_snapshots, string.Empty,
            "sam_cph_holdings/sam_cph_holdings_20260819203115.parquet",
            "cts_keeper/cts_keeper_20260819203115.parquet");

        var result = await Controller().ListObjects("snapshots", "all");

        var response = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStorageReportResponse>().Subject;
        response.ObjectCount.Should().Be(2);
        response.Objects.Select(o => $"{o.Stage}/{o.Key}").Should().Equal(
            "snapshots/cts_keeper/cts_keeper_20260819203115.parquet",
            "snapshots/sam_cph_holdings/sam_cph_holdings_20260819203115.parquet");
        response.Groups.Select(g => g.Dataset).Should().Equal("cts_keeper", "sam_cph_holdings");
        response.Groups.Should().OnlyContain(g => g.ObjectCount == 1);
    }

    [Fact]
    public async Task Report_sums_size_for_the_folder_and_each_dataset()
    {
        LaneObjects(_snapshots, string.Empty,
            Object("sam_cph_holdings/a.parquet", 100),
            Object("sam_cph_holdings/b.parquet", 250),
            Object("cts_keeper/c.parquet", 50));

        var result = await Controller().ListObjects("snapshots", "all");

        var response = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStorageReportResponse>().Subject;
        response.ObjectCount.Should().Be(3);
        response.TotalSizeBytes.Should().Be(400);
        response.Groups.Should().ContainSingle(g =>
            g.Dataset == "sam_cph_holdings" && g.ObjectCount == 2 && g.SizeBytes == 350);
        response.Groups.Should().ContainSingle(g =>
            g.Dataset == "cts_keeper" && g.ObjectCount == 1 && g.SizeBytes == 50);
    }

    [Fact]
    public async Task Report_scopes_the_listing_to_the_dataset_its_keys_live_under()
    {
        var result = await Controller().ListObjects("snapshots", "sam_cph_holdings");

        result.Should().BeOfType<OkObjectResult>();
        _snapshots.Verify(s => s.EnumerateAsync(
            "sam_cph_holdings/", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Report_folds_every_folder_together_for_stage_all()
    {
        LaneObjects(_raw, null, Object("litprd/LITP_SAMCPHHOLDING_20260819203115.psv", 10));
        LaneObjects(_normalised, null, Object("sam_cph_holdings/a.parquet", 20));
        LaneObjects(_staging, null, Object("krds-db.duckdb", 30));
        LaneObjects(_views, null, Object("krds-db.sqlite", 40));

        var result = await Controller().ListObjects("all", "all");

        var response = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStorageReportResponse>().Subject;
        response.ObjectCount.Should().Be(4);
        response.TotalSizeBytes.Should().Be(100);
        response.Objects.Select(o => $"{o.Stage}/{o.Key}").Should().Equal(
            "normalised/sam_cph_holdings/a.parquet",
            "raw/litprd/LITP_SAMCPHHOLDING_20260819203115.psv",
            "staging/krds-db.duckdb",
            "views/krds-db.sqlite");
        response.Groups.Should().ContainSingle(g => g.Dataset == "shared" && g.ObjectCount == 2);
        response.Groups.Should().ContainSingle(g =>
            g.Dataset == "sam_cph_holdings" && g.ObjectCount == 2);
    }

    [Fact]
    public async Task Report_matches_cts_keys_by_pattern_not_prefix()
    {
        LaneObjects(_raw, "cads/cts/bulk/",
            Object("cads/cts/bulk/CTSM_CADS_PREP_BULK_00001_001_CT_LOCATIONS_2026-07-28-094638.psv", 5),
            Object("cads/cts/bulk/CTSM_CADS_PREP_BULK_00001_001_CT_LOCATION_PARTY_RELS_2026-07-28-094630.psv", 7));

        var result = await Controller().ListObjects("raw", "cts_locations");

        var response = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStorageReportResponse>().Subject;
        response.Objects.Select(o => o.Key).Should().Equal(
            "cads/cts/bulk/CTSM_CADS_PREP_BULK_00001_001_CT_LOCATIONS_2026-07-28-094638.psv");
        response.Groups.Should().ContainSingle(g =>
            g.Dataset == "cts_locations" && g.ObjectCount == 1 && g.SizeBytes == 5);
    }

    [Fact]
    public async Task Report_pages_the_object_slice_while_totals_cover_the_whole_listing()
    {
        LaneObjects(_snapshots, string.Empty,
            Object("a/1.parquet", 1),
            Object("a/2.parquet", 1),
            Object("a/3.parquet", 1));

        var result = await Controller().ListObjects("snapshots", "all", "internal", skip: 1, top: 1);

        var response = result.Should().BeOfType<OkObjectResult>()
            .Which.Value.Should().BeOfType<EtlStorageReportResponse>().Subject;
        response.ObjectCount.Should().Be(3);
        response.TotalSizeBytes.Should().Be(3);
        response.Objects.Select(o => o.Key).Should().Equal("a/2.parquet");
        response.Skip.Should().Be(1);
        response.Top.Should().Be(1);
    }

    [Theory]
    [InlineData("staging")]
    [InlineData("views")]
    public async Task Dataset_scoped_shared_folder_report_is_rejected_because_the_folder_is_shared(string stage)
    {
        var result = await Controller().ListObjects(stage, "sam_cph_holdings");

        result.Should().BeOfType<BadRequestObjectResult>()
            .Which.Value.Should().BeOfType<ErrorResponse>()
            .Which.Message.Should().Contain("shared across every dataset");
        _staging.VerifyNoOtherCalls();
        _views.VerifyNoOtherCalls();
    }

    [Theory]
    [InlineData("other", "all", "internal", 0, 100, "Invalid stage")]
    [InlineData("raw", "unknown", "internal", 0, 100, "not recognized")]
    [InlineData("raw", "all", "internet", 0, 100, "Invalid sourceType")]
    [InlineData("raw", "all", "internal", -1, 100, "Skip must be")]
    [InlineData("raw", "all", "internal", 0, 0, "Top must be")]
    [InlineData("raw", "all", "internal", 0, 1001, "Top must be")]
    public async Task Report_rejects_invalid_parameters(
        string stage,
        string dataset,
        string sourceType,
        int skip,
        int top,
        string expectedMessage)
    {
        var result = await Controller().ListObjects(stage, dataset, sourceType, skip, top);

        result.Should().BeOfType<BadRequestObjectResult>()
            .Which.Value.Should().BeOfType<ErrorResponse>()
            .Which.Message.Should().Contain(expectedMessage);
    }

    [Fact]
    public async Task Report_is_allowed_in_production_because_it_only_reads()
    {
        _environment.SetupGet(e => e.EnvironmentName).Returns("Production");
        LaneObjects(_raw, null, Object("raw.psv", 1));

        var result = await Controller().ListObjects("raw", "all");

        result.Should().BeOfType<OkObjectResult>();
    }

    [Fact]
    public async Task Report_failure_returns_a_safe_500_message()
    {
        _raw.Setup(s => s.EnumerateAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Failed());

        var result = await Controller().ListObjects("raw", "all");

        var failed = result.Should().BeOfType<ObjectResult>().Subject;
        failed.StatusCode.Should().Be(StatusCodes.Status500InternalServerError);
        failed.Value.Should().BeOfType<ErrorResponse>()
            .Which.Message.Should().Be("Failed to list S3 stage storage.");
    }

    private EtlStorageController Controller()
    {
        var definitions = new Mock<IDataSetDefinitions>();
        definitions.SetupGet(d => d.All).Returns(StandardDataSetDefinitionsBuilder.Build().All);

        return new EtlStorageController(
            _blobFactory.Object,
            _storageProvider.Object,
            definitions.Object,
            _statusStore.Object,
            _environment.Object,
            Options.Create(new FeatureFlags
            {
                EtlStoragePurgeEnabled = _etlStoragePurgeEnabled
            }),
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

    /// <summary>A dataset named by a pattern is purged by streaming its lanes and matching keys, not by
    /// deleting under a prefix.</summary>
    private static void Lane(
        Mock<IBlobStorageService> storage,
        string prefix,
        params string[] keys)
        => LaneObjects(storage, prefix, keys.Select(k => Object(k)).ToArray());

    private static void LaneObjects(
        Mock<IBlobStorageService> storage,
        string? prefix,
        params StorageObjectInfo[] objects)
        => storage.Setup(s => s.EnumerateAsync(prefix ?? string.Empty, It.IsAny<CancellationToken>()))
            .Returns(Streamed(objects));

    private static async IAsyncEnumerable<StorageObjectInfo> Streamed(StorageObjectInfo[] objects)
    {
        foreach (var item in objects)
        {
            yield return item;
        }

        await Task.CompletedTask;
    }

    private static async IAsyncEnumerable<StorageObjectInfo> Failed()
    {
        yield return Object("never");
        await Task.CompletedTask;
        throw new InvalidOperationException("storage went away");
    }

    private static StorageObjectInfo Object(string key, long size = 0) => new()
    {
        Container = "internal",
        Key = key,
        Size = size,
        StorageUri = new Uri($"s3://internal/{key}")
    };
}
