using FluentAssertions;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.EtlPipeline.Views;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;
using Microsoft.Extensions.Logging.Abstractions;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>The export stage's contract: when it builds, when it skips, and what it never leaves
/// behind. The transformation itself is the writer's business and is not exercised here.</summary>
public class ExportSqliteStageTests
{
    private static readonly DateTimeOffset SourceTimestamp = new(2026, 8, 21, 7, 0, 3, TimeSpan.Zero);

    private const string ExpectedKey = "krds-db_20260821070003.sqlite";
    private const string StagingKey = "keeper_data_bridge_20260821070003.duckdb";

    private readonly InMemoryEtlPipelineStorage _storage = new();
    private readonly RecordingSqliteViewWriter _writer = new();

    private InMemoryBlobStorage Staging => _storage.Folder(EtlPipelineFolders.Staging);
    private InMemoryBlobStorage Views => _storage.Folder(EtlPipelineFolders.Views);

    public ExportSqliteStageTests() => Staging.Put(StagingKey, "a staging database");

    private Task<List<SqliteExportFile>> RunAsync(params StagingDatabase[] inputs)
        => StageRunner.RunAsync(
            new ExportSqliteStage(_storage, _writer, NullLogger<ExportSqliteStage>.Instance), inputs);

    private static StagingDatabase Database(bool created = true) => new()
    {
        RunId = Guid.NewGuid(),
        Key = StagingKey,
        SourceTimestamp = SourceTimestamp,
        Created = created
    };

    [Fact]
    public async Task Names_the_export_after_the_staging_databases_source_timestamp()
    {
        var output = await RunAsync(Database());

        output.Should().ContainSingle().Which.Key.Should().Be(ExpectedKey);
        Views.Keys.Should().ContainSingle().Which.Should().Be(ExpectedKey);
    }

    [Fact]
    public async Task Produces_nothing_when_the_pipeline_produced_no_staging_database()
    {
        var output = await RunAsync();

        output.Should().BeEmpty();
        Views.Keys.Should().BeEmpty();
    }

    [Fact]
    public async Task Produces_nothing_for_an_empty_staging_database_payload()
    {
        var output = await RunAsync(new StagingDatabase());

        output.Should().BeEmpty();
        Views.Keys.Should().BeEmpty();
        _writer.Calls.Should().BeEmpty();
    }

    [Fact]
    public async Task Exports_from_a_staging_database_that_was_reused_rather_than_rebuilt()
    {
        // A reused database reports no tables, but it is still there to export from - gating on the
        // table count would skip every re-run.
        var output = await RunAsync(Database(created: false));

        output.Should().ContainSingle().Which.Created.Should().BeTrue();
        Views.Keys.Should().ContainSingle();
    }

    [Fact]
    public async Task Reports_the_row_counts_the_writer_produced()
    {
        _writer.Tables = [new SqliteViewTable("Party", 162_981), new SqliteViewTable("Holding", 111_247)];

        var output = await RunAsync(Database());

        output.Single().Tables.Should().BeEquivalentTo(_writer.Tables);
    }

    [Fact]
    public async Task Reuses_an_export_built_by_the_same_transformation()
    {
        await RunAsync(Database());
        _writer.Calls.Clear();

        var output = await RunAsync(Database());

        output.Single().Created.Should().BeFalse();
        output.Single().Tables.Should().BeEquivalentTo(_writer.Tables,
            "reused exports still need reconciliation counts in import status");
        _writer.Calls.Should().BeEmpty("the file is already current, so the transformation should not run again");
    }

    [Fact]
    public async Task Rebuilds_an_export_left_by_an_earlier_transformation()
    {
        Views.Put(ExpectedKey, "built by an older script", new Dictionary<string, string>
        {
            [ViewsFileNaming.VersionMetadataKey] = "v1-0000000000000000"
        });

        var output = await RunAsync(Database());

        output.Single().Created.Should().BeTrue();
        _writer.Calls.Should().ContainSingle("a changed transformation must take effect for a timestamp already exported");
        Views.MetadataOf(ExpectedKey)[ViewsFileNaming.VersionMetadataKey].Should().Be(SqliteViewDefinition.Version);
    }

    [Fact]
    public async Task Rebuilds_an_export_that_carries_no_version_at_all()
    {
        Views.Put(ExpectedKey, "written before versions were recorded");

        var output = await RunAsync(Database());

        output.Single().Created.Should().BeTrue();
        _writer.Calls.Should().ContainSingle();
    }

    [Fact]
    public async Task Rebuilds_a_current_export_that_has_no_table_counts()
    {
        Views.Put(ExpectedKey, "missing reconciliation metadata", new Dictionary<string, string>
        {
            [ViewsFileNaming.VersionMetadataKey] = SqliteViewDefinition.Version
        });

        var output = await RunAsync(Database());

        output.Single().Created.Should().BeTrue();
        _writer.Calls.Should().ContainSingle();
    }

    [Fact]
    public async Task Uploads_nothing_when_the_transformation_fails()
    {
        _writer.Failure = new InvalidOperationException("Binder Error: no such table sam_party");

        var act = async () => await RunAsync(Database());

        await act.Should().ThrowAsync<InvalidOperationException>();
        Views.Keys.Should().BeEmpty("a failed transformation must not publish a read model");
    }

    [Fact]
    public async Task Hands_the_writer_the_embedded_transformation_and_its_tables()
    {
        await RunAsync(Database());

        var request = _writer.Calls.Single();
        request.Sql.Should().Be(SqliteViewDefinition.Sql);
        request.TableNames.Should().BeEquivalentTo(SqliteViewDefinition.TableNames);
        request.SourceDatabasePath.Should().EndWith(".duckdb");
        request.TargetDatabasePath.Should().EndWith(".sqlite");
    }

    [Fact]
    public async Task Leaves_no_working_directory_behind()
    {
        await RunAsync(Database());

        Directory.Exists(Path.GetDirectoryName(_writer.Calls.Single().TargetDatabasePath)!)
            .Should().BeFalse("ephemeral task storage is finite and the run must clean up after itself");
    }

    /// <summary>Stands in for DuckDB: records what it was asked to do and writes a file so the stage
    /// has something to upload.</summary>
    private sealed class RecordingSqliteViewWriter : ISqliteViewWriter
    {
        public List<SqliteViewWriteRequest> Calls { get; } = [];

        public IReadOnlyList<SqliteViewTable> Tables { get; set; } =
            [.. SqliteViewDefinition.TableNames.Select((name, index) => new SqliteViewTable(name, index + 1))];

        public Exception? Failure { get; set; }

        public Task<SqliteViewWriteResult> WriteAsync(
            SqliteViewWriteRequest request,
            CancellationToken cancellationToken = default)
        {
            Calls.Add(request);

            if (Failure is not null) throw Failure;

            File.WriteAllText(request.TargetDatabasePath, "a sqlite database");

            return Task.FromResult(new SqliteViewWriteResult(Tables));
        }
    }
}
