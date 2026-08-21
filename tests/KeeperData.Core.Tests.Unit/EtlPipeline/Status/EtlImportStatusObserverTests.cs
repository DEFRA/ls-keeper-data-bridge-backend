using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Staging;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.EtlPipeline.Status;
using KeeperData.Core.Pipeline;
using KeeperData.Core.Storage.Dtos;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;
using Microsoft.Extensions.Logging.Abstractions;
using System.Security.Cryptography;

namespace KeeperData.Core.Tests.Unit.EtlPipeline.Status;

/// <summary>The observer is the only thing that knows how to read import status out of what the
/// stages emit, so these tests are the contract for every field the status API serves.</summary>
public class EtlImportStatusObserverTests
{
    private readonly RecordingEtlImportStatusStore _store = new();
    private readonly Guid _importId = Guid.NewGuid();

    private EtlImportStatusObserver Sut() => new(_store, NullLogger<EtlImportStatusObserver>.Instance);

    private EtlPipelineContext Context() => new(_importId, "external");

    private Task StageCompleted(string stage, params object[] items)
        => Sut().StageCompletedAsync(Context(), stage, items, TimeSpan.FromSeconds(2), CancellationToken.None);

    [Fact]
    public async Task Marks_the_import_running_when_the_pipeline_starts()
    {
        await Sut().RunStartingAsync(Context(), ["discover", "decrypt"], CancellationToken.None);

        _store.Started.Should().ContainSingle()
            .Which.Should().BeEquivalentTo((_importId, new[] { "discover", "decrypt" }));
    }

    [Fact]
    public async Task Records_the_discovered_source_files_and_their_sizes()
    {
        var definition = StageRunner.Definition("sam_cph_holdings");
        var key = "LITP_SAMCPHHOLDING_20251113121333.csv";

        var file = new EtlFile(
            new StorageObjectInfo
            {
                Container = "external",
                Key = key,
                Size = 4096,
                StorageUri = new Uri($"s3://external/{key}")
            },
            DateTimeOffset.UtcNow);

        await StageCompleted("discover", new DiscoveredFileSet(definition, [file]));

        var dataset = _store.Progress.Single().Progress.Datasets.Single();
        dataset.Dataset.Should().Be("sam_cph_holdings");
        dataset.SourceFiles.Should().Equal(("LITP_SAMCPHHOLDING_20251113121333.csv", 4096L));
    }

    [Fact]
    public async Task Records_the_raw_and_normalised_keys_each_stage_wrote()
    {
        var definition = StageRunner.Definition("sam_cph_holdings");

        await StageCompleted("decrypt", new RawFileSet(definition) { Files = ["sam_cph_holdings/a.csv"] });
        await StageCompleted("normalise", new NormalisedFileSet(definition) { Files = ["sam_cph_holdings/a.parquet"] });

        _store.Progress[0].Progress.Datasets.Single().RawKeys.Should().Equal("sam_cph_holdings/a.csv");
        _store.Progress[1].Progress.Datasets.Single().NormalisedKeys.Should().Equal("sam_cph_holdings/a.parquet");
    }

    [Fact]
    public async Task Records_the_snapshot_and_its_row_counts()
    {
        var timestamp = new DateTimeOffset(2025, 11, 15, 12, 13, 33, TimeSpan.Zero);

        await StageCompleted("snapshot", new SnapshotFile(StageRunner.Definition("sam_cph_holdings"))
        {
            Key = "sam_cph_holdings/sam_cph_holdings_20251115121333.parquet",
            SourceTimestamp = timestamp,
            RowCount = 12345,
            RowsUpserted = 42,
            RowsIgnoredDeletes = 7
        });

        var dataset = _store.Progress.Single().Progress.Datasets.Single();
        dataset.SnapshotKey.Should().Be("sam_cph_holdings/sam_cph_holdings_20251115121333.parquet");
        dataset.SnapshotSourceTimestamp.Should().Be(timestamp);
        dataset.RowCount.Should().Be(12345);
        dataset.RowsUpserted.Should().Be(42);
        dataset.RowsIgnoredDeletes.Should().Be(7);
    }

    [Fact]
    public async Task Records_the_columns_the_snapshot_drifted_on()
    {
        await StageCompleted("snapshot", new SnapshotFile(StageRunner.Definition("sam_cph_holdings"))
        {
            Key = "sam_cph_holdings/sam_cph_holdings_20251115121333.parquet",
            ColumnsNullified = ["ADDRESS_PK"],
            ColumnsAdded = ["NEW_COLUMN"]
        });

        var dataset = _store.Progress.Single().Progress.Datasets.Single();
        dataset.ColumnsNullified.Should().Equal("ADDRESS_PK");
        dataset.ColumnsAdded.Should().Equal("NEW_COLUMN");
    }

    [Fact]
    public async Task Records_the_staging_database_key()
    {
        await StageCompleted("load-duckdb", new StagingDatabase
        {
            Key = "keeper_data_bridge_20251115121333.duckdb",
            Tables = [new StagingTable("sam_cph_holdings", "sam_cph_holdings/x.parquet", 3)]
        });

        _store.Progress.Single().Progress.DuckDbKey.Should().Be("keeper_data_bridge_20251115121333.duckdb");
    }

    [Fact]
    public async Task Keeps_each_dataset_separate_when_a_stage_emits_several()
    {
        await StageCompleted(
            "snapshot",
            new SnapshotFile(StageRunner.Definition("sam_cph_holdings")) { Key = "a.parquet", RowCount = 1 },
            new SnapshotFile(StageRunner.Definition("cts_keeper")) { Key = "b.parquet", RowCount = 2 });

        _store.Progress.Single().Progress.Datasets
            .Select(d => (d.Dataset, d.RowCount))
            .Should().BeEquivalentTo(new[] { ("sam_cph_holdings", 1L), ("cts_keeper", 2L) });
    }

    [Fact]
    public async Task Ignores_payloads_it_does_not_recognise()
    {
        await StageCompleted("source", "not a payload", 42);

        _store.Progress.Single().Progress.Datasets.Should().BeEmpty();
    }

    [Fact]
    public async Task Marks_the_import_succeeded_when_the_run_completes()
    {
        await Sut().RunCompletedAsync(Context(), TimeSpan.FromMinutes(1), CancellationToken.None);

        _store.Succeeded.Should().Equal(_importId);
    }

    [Fact]
    public async Task Records_the_underlying_cause_rather_than_the_pipeline_wrapper()
    {
        var failure = new PipelineExecutionException(
            "Pipeline failed after 10ms.",
            new InvalidOperationException("snapshot timestamp could not be parsed"));

        await Sut().RunFailedAsync(Context(), failure, CancellationToken.None);

        _store.Failed.Single().Error
            .Should().Be("InvalidOperationException: snapshot timestamp could not be parsed");
    }

    [Fact]
    public async Task Records_a_stages_explanation_rather_than_the_cause_it_wrapped()
    {
        var failure = new PipelineExecutionException(
            "Pipeline failed after 10ms.",
            new SourceFileDecryptionException(
                "LITP_SAMCPHHOLDING_20260811074324.csv",
                "sam_cph_holdings",
                new CryptographicException("Padding is invalid and cannot be removed.")));

        await Sut().RunFailedAsync(Context(), failure, CancellationToken.None);

        var error = _store.Failed.Single().Error;

        error.Should().StartWith("Could not decrypt 'LITP_SAMCPHHOLDING_20260811074324.csv' for dataset 'sam_cph_holdings'.");
        error.Should().NotContain("Padding", "the padding error is what the explanation exists to replace");
        error.Should().NotContain("SourceFileDecryptionException", "the message was written to be read as it is");
    }
}
