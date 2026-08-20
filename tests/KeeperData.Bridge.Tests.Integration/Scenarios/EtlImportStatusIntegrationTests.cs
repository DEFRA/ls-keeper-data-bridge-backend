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
/// the ETL pipeline over LocalStack, observed into a real Mongo collection.
///
/// The unit tests cover what the observer derives from each payload; these cover that it survives
/// the round trip through Mongo, which is where the status API reads it from.
/// </summary>
[Collection("LocalStack"), Trait("Dependence", "docker")]
public sealed class EtlImportStatusIntegrationTests(LocalStackFixture localStack) : IAsyncLifetime
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
        dataset.ColumnsNullified.Should().BeEmpty("nothing drifted, so the run says nothing about columns");
        dataset.ColumnsAdded.Should().BeEmpty();

        status.DuckDbKey.Should().Be("keeper_data_bridge_20251113121333.duckdb");
    }

    /// <summary>Schema drift is tolerated, so nothing fails and nothing is in the caller's face; the run
    /// has to say which columns it changed the shape of, or the only record of it is a log line.</summary>
    [Fact]
    public async Task A_run_that_tolerated_schema_drift_records_the_columns_it_drifted_on()
    {
        var importId = Guid.NewGuid();

        await using var host = await CreateHostAsync();

        await host.PutEncryptedSourceFileAsync(SourceFile, new StringBuilder()
            .AppendLine(Header.Replace("|CHANGE_TYPE", "|ADDRESS_PK|CHANGE_TYPE"))
            .AppendLine($"01/001/0001|{KeyColumns}|Keep Farm|ADDR001|I")
            .ToString());

        await host.PutEncryptedSourceFileAsync("LITP_SAMCPHHOLDING_20251113131333.csv", new StringBuilder()
            .AppendLine($"{Header}|NEW_COLUMN")
            .AppendLine($"01/001/0002|{KeyColumns}|Other Farm|I|VALUE")
            .ToString());

        await _store.CreateQueuedAsync(importId, "external", "sam_cph_holdings", CancellationToken.None);
        await host.RunPipelineAsync(runId: importId, dataset: "sam_cph_holdings");

        var status = (await _store.GetAsync(importId, CancellationToken.None))!;

        status.Status.Should().Be(nameof(EtlImportStatus.Succeeded));

        var dataset = status.Datasets.Should().ContainSingle().Subject;

        dataset.ColumnsNullified.Should().Equal("ADDRESS_PK");
        dataset.ColumnsAdded.Should().Equal("NEW_COLUMN");
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
        status.Error.Should().NotContain(EtlPipelineTestHost.AesSalt, "a status a caller can read must never carry the salt");
        status.Error.Should().NotContain(" at ", "the caller gets a summary; the stack trace stays in the logs");
    }

    [Fact]
    public async Task A_file_encrypted_for_another_environment_reports_which_file_and_why_rather_than_a_padding_error()
    {
        var importId = Guid.NewGuid();

        await using var host = await CreateHostAsync();

        // Correctly named and inside the lookback window, but encrypted against a different salt -
        // which is what a caller hits when they are handed fixtures built for another environment.
        await host.PutEncryptedSourceFileAsync(SourceFile, SourceContent(), salt: "a-different-environments-salt");

        await _store.CreateQueuedAsync(importId, "external", null, CancellationToken.None);

        var run = async () => await host.RunPipelineAsync(runId: importId);
        await run.Should().ThrowAsync<Exception>();

        var status = (await _store.GetAsync(importId, CancellationToken.None))!;

        status.Status.Should().Be(nameof(EtlImportStatus.Failed));
        status.Error.Should().Contain(SourceFile, "the reader needs to know which file could not be decrypted");
        status.Error.Should().Contain("filename is the decryption password");
        status.Error.Should().NotContain("Padding", "the padding error is what this message exists to replace");
        status.Error.Should().NotContain(EtlPipelineTestHost.AesSalt, "a status a caller can read must never carry the salt");
    }

    [Fact]
    public async Task A_run_that_follows_a_failed_decryption_of_the_same_file_succeeds_once_the_file_is_replaced()
    {
        await using var host = await CreateHostAsync();

        // A wrong-salt file writes most of a raw object before AES rejects the final block, and the
        // write commits on the way out of the failure. Left there, the retry below would skip
        // decryption as already done and normalise the garbage instead - surfacing much later as a
        // parquet file with none of the dataset's columns in it.
        await host.PutEncryptedSourceFileAsync(SourceFile, SourceContent(), salt: "a-different-environments-salt");

        var failedImportId = Guid.NewGuid();
        await _store.CreateQueuedAsync(failedImportId, "external", null, CancellationToken.None);

        var failedRun = async () => await host.RunPipelineAsync(runId: failedImportId);
        await failedRun.Should().ThrowAsync<Exception>();

        (await host.ListFolderAsync("raw")).Should().BeEmpty("a raw file that was never finished must not be left behind");

        await host.PutEncryptedSourceFileAsync(SourceFile, SourceContent());

        var retryImportId = Guid.NewGuid();
        await _store.CreateQueuedAsync(retryImportId, "external", null, CancellationToken.None);

        await host.RunPipelineAsync(runId: retryImportId);

        var status = (await _store.GetAsync(retryImportId, CancellationToken.None))!;

        status.Status.Should().Be(nameof(EtlImportStatus.Succeeded));
        status.Datasets.Should().ContainSingle().Which.RowCount.Should().Be(2);
    }

    [Fact]
    public async Task An_import_that_stopped_reporting_progress_is_reported_as_failed_rather_than_running_forever()
    {
        var importId = Guid.NewGuid();
        var clock = new Microsoft.Extensions.Time.Testing.FakeTimeProvider(RunClock);

        var store = CreateStore(clock);

        await store.CreateQueuedAsync(importId, "external", null, CancellationToken.None);
        await store.MarkRunningAsync(importId, ["discover"], CancellationToken.None);

        clock.Advance(MongoEtlImportStatusStore.LeaseDuration + TimeSpan.FromMinutes(1));

        (await store.GetAsync(importId, CancellationToken.None))!.Status.Should().Be(nameof(EtlImportStatus.Failed));
        (await store.GetInFlightAsync(CancellationToken.None))?.ImportId
            .Should().NotBe(importId, "an abandoned run must not block the next import forever");
    }

    [Fact]
    public async Task Listing_imports_returns_the_most_recently_requested_first_with_the_total_to_page_through()
    {
        var clock = new Microsoft.Extensions.Time.Testing.FakeTimeProvider(RunClock);
        var store = CreateStore(clock);

        var ids = new List<Guid>();

        for (var i = 0; i < 5; i++)
        {
            var importId = Guid.NewGuid();
            ids.Add(importId);

            await store.CreateQueuedAsync(importId, "internal", "sam_cph_holdings", CancellationToken.None);
            clock.Advance(TimeSpan.FromMinutes(1));
        }

        var firstPage = await store.ListAsync(0, 2, CancellationToken.None);

        firstPage.TotalCount.Should().Be(5, "the total is what lets a caller page without walking to the end");
        firstPage.Imports.Select(i => i.ImportId).Should().Equal(ids[4], ids[3]);

        var secondPage = await store.ListAsync(2, 2, CancellationToken.None);

        secondPage.TotalCount.Should().Be(5);
        secondPage.Imports.Select(i => i.ImportId).Should().Equal(ids[2], ids[1]);

        var pastTheEnd = await store.ListAsync(10, 2, CancellationToken.None);

        pastTheEnd.Imports.Should().BeEmpty();
        pastTheEnd.TotalCount.Should().Be(5);
    }

    [Fact]
    public async Task Listing_imports_reports_an_abandoned_run_as_failed_the_same_way_polling_does()
    {
        var clock = new Microsoft.Extensions.Time.Testing.FakeTimeProvider(RunClock);
        var store = CreateStore(clock);

        var importId = Guid.NewGuid();

        await store.CreateQueuedAsync(importId, "internal", null, CancellationToken.None);
        await store.MarkRunningAsync(importId, ["discover"], CancellationToken.None);

        clock.Advance(MongoEtlImportStatusStore.LeaseDuration + TimeSpan.FromMinutes(1));

        var page = await store.ListAsync(0, 10, CancellationToken.None);

        page.Imports.Should().ContainSingle()
            .Which.Status.Should().Be(nameof(EtlImportStatus.Failed));
    }

    [Fact]
    public async Task An_unknown_import_id_has_no_status()
    {
        (await _store.GetAsync(Guid.NewGuid(), CancellationToken.None)).Should().BeNull();
    }

    private MongoEtlImportStatusStore CreateStore(TimeProvider clock)
        => new(
            new MongoClient(_mongo.GetConnectionString()),
            Options.Create<IDatabaseConfig>(new TestDatabaseConfig()),
            clock,
            NullLogger<MongoEtlImportStatusStore>.Instance);

    private Task<EtlPipelineTestHost> CreateHostAsync()
        => EtlPipelineTestHost.CreateAsync(localStack.S3Client, RunClock, statusStore: _store);

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
