using System.Text;
using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.Database;
using KeeperData.Core.EtlPipeline.Status;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Testcontainers.MongoDb;

namespace KeeperData.Bridge.Tests.Integration.Scenarios;

/// <summary>
/// The status a QA analyst polls, produced the way it will be in the deployed system: a real run of
/// the file-based pipeline over LocalStack, observed into a real Mongo collection.
///
/// The unit tests cover what the observer derives from each payload; these cover that it survives
/// the round trip through Mongo, which is where the status API reads it from.
/// </summary>
[Collection("LocalStack"), Trait("Dependence", "docker")]
public sealed class FileBasedImportStatusIntegrationTests(LocalStackFixture localStack) : IAsyncLifetime
{
    private const string Header = "CPH|FEATURE_NAME|SECONDARY_CPH|ANIMAL_SPECIES_CODE|HOLDING_NAME|CHANGE_TYPE";
    private const string KeyColumns = "MAIN|-|01";
    private const string SourceFile = "LITP_SAMCPHHOLDING_20251113121333.csv";

    private static readonly DateTimeOffset RunClock = new(2025, 11, 13, 18, 0, 0, TimeSpan.Zero);

    private MongoDbContainer _mongo = null!;
    private MongoEtlImportStatusStore _store = null!;

    public async Task InitializeAsync()
    {
        _mongo = new MongoDbBuilder().WithImage("mongo:7.0").WithPortBinding(27017, true).Build();
        await _mongo.StartAsync();

        _store = new MongoEtlImportStatusStore(
            new MongoClient(_mongo.GetConnectionString()),
            Options.Create<IDatabaseConfig>(new TestDatabaseConfig()),
            TimeProvider.System,
            NullLogger<MongoEtlImportStatusStore>.Instance);
    }

    public Task DisposeAsync() => _mongo.DisposeAsync().AsTask();

    [Fact]
    public async Task A_successful_run_records_every_output_path_the_QA_analyst_needs()
    {
        var importId = Guid.NewGuid();

        await using var host = await CreateHostAsync();
        await host.PutEncryptedSourceFileAsync(SourceFile, SourceContent());

        await _store.CreateQueuedAsync(importId, "external", "sam_cph_holdings", CancellationToken.None);

        (await _store.GetAsync(importId, CancellationToken.None))!.Status
            .Should().Be(nameof(EtlImportStatus.Queued), "the document exists before the run starts, so a poll immediately after the trigger finds it");

        await host.RunPipelineAsync(runId: importId, dataset: "sam_cph_holdings");

        var status = (await _store.GetAsync(importId, CancellationToken.None))!;

        status.Status.Should().Be(nameof(EtlImportStatus.Succeeded));
        status.Dataset.Should().Be("sam_cph_holdings");
        status.StartedAtUtc.Should().NotBeNull();
        status.CompletedAtUtc.Should().NotBeNull();
        status.CurrentStage.Should().BeNull("a finished run is not in a stage");
        status.Error.Should().BeNull();

        status.Stages.Select(s => s.Name)
            .Should().Equal("discover", "decrypt", "normalise", "snapshot", "load-duckdb");

        var dataset = status.Datasets.Should().ContainSingle().Subject;

        dataset.SourceFiles.Should().ContainSingle().Which.Key.Should().Be(SourceFile);
        dataset.SourceFiles.Single().Size.Should().BePositive();

        dataset.RawKeys.Should().Equal(SourceFile);
        dataset.NormalisedKeys.Should().Equal("sam_cph_holdings/LITP_SAMCPHHOLDING_20251113121333.parquet");
        dataset.SnapshotKey.Should().Be("sam_cph_holdings/sam_cph_holdings_20251113121333.parquet");
        dataset.RowCount.Should().Be(2);
        dataset.RowsUpserted.Should().Be(2);
        dataset.RowsIgnoredDeletes.Should().Be(0);

        status.DuckDbKey.Should().Be("keeper_data_bridge_20251113121333.duckdb");
    }

    [Fact]
    public async Task A_failed_run_records_a_safe_summary_rather_than_a_stack_trace()
    {
        var importId = Guid.NewGuid();

        await using var host = await CreateHostAsync();

        // A source file whose name carries no parsable timestamp fails the snapshot stage.
        await host.PutEncryptedSourceFileAsync("LITP_SAMCPHHOLDING_NOTATIMESTAMP.csv", SourceContent());

        await _store.CreateQueuedAsync(importId, "external", null, CancellationToken.None);

        var run = async () => await host.RunPipelineAsync(runId: importId);
        await run.Should().ThrowAsync<Exception>();

        var status = (await _store.GetAsync(importId, CancellationToken.None))!;

        status.Status.Should().Be(nameof(EtlImportStatus.Failed));
        status.CompletedAtUtc.Should().NotBeNull();
        status.Error.Should().NotBeNullOrWhiteSpace();
        status.Error.Should().NotContain(FileBasedPipelineTestHost.AesSalt, "a status a caller can read must never carry the salt");
        status.Error.Should().NotContain(" at ", "the caller gets a summary; the stack trace stays in the logs");
    }

    [Fact]
    public async Task An_import_that_stopped_reporting_progress_is_reported_as_failed_rather_than_running_forever()
    {
        var importId = Guid.NewGuid();
        var clock = new Microsoft.Extensions.Time.Testing.FakeTimeProvider(RunClock);

        var store = new MongoEtlImportStatusStore(
            new MongoClient(_mongo.GetConnectionString()),
            Options.Create<IDatabaseConfig>(new TestDatabaseConfig()),
            clock,
            NullLogger<MongoEtlImportStatusStore>.Instance);

        await store.CreateQueuedAsync(importId, "external", null, CancellationToken.None);
        await store.MarkRunningAsync(importId, ["discover"], CancellationToken.None);

        clock.Advance(MongoEtlImportStatusStore.LeaseDuration + TimeSpan.FromMinutes(1));

        (await store.GetAsync(importId, CancellationToken.None))!.Status.Should().Be(nameof(EtlImportStatus.Failed));
        (await store.GetInFlightAsync(CancellationToken.None))?.ImportId
            .Should().NotBe(importId, "an abandoned run must not block the next import forever");
    }

    [Fact]
    public async Task An_unknown_import_id_has_no_status()
    {
        (await _store.GetAsync(Guid.NewGuid(), CancellationToken.None)).Should().BeNull();
    }

    private Task<FileBasedPipelineTestHost> CreateHostAsync()
        => FileBasedPipelineTestHost.CreateAsync(localStack.S3Client, RunClock, statusStore: _store);

    private static string SourceContent() => new StringBuilder()
        .AppendLine(Header)
        .AppendLine($"01/001/0001|{KeyColumns}|Keep Farm|I")
        .AppendLine($"01/001/0002|{KeyColumns}|Other Farm|I")
        .ToString();

    private sealed class TestDatabaseConfig : IDatabaseConfig
    {
        public string DatabaseName => "etl-status-tests";
    }
}
