using System.Text;
using FluentAssertions;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline;
using KeeperData.Core.EtlPipeline.Payloads;
using KeeperData.Core.EtlPipeline.Staging;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Tests.Unit.EtlPipeline.Harness;
using Microsoft.Extensions.Logging.Abstractions;

namespace KeeperData.Core.Tests.Unit.EtlPipeline;

/// <summary>Load. Input: SnapshotFile (all of them). Output: one StagingDatabase.
/// Every snapshot becomes a table in the one database, named after the newest source timestamp
/// loaded. The database engine itself is exercised in the infrastructure tests.</summary>
public class LoadDuckDbStageTests
{
    private readonly InMemoryEtlPipelineStorage _storage = new();
    private readonly RecordingStagingDatabaseWriter _writer = new();

    private InMemoryBlobStorage Snapshots => _storage.Folder(EtlPipelineFolders.Snapshots);
    private InMemoryBlobStorage Staging => _storage.Folder(EtlPipelineFolders.Staging);

    private Task<List<StagingDatabase>> RunAsync(params SnapshotFile[] inputs) =>
        StageRunner.RunAsync(
            new LoadDuckDbStage(_storage, _writer, NullLogger<LoadDuckDbStage>.Instance),
            inputs);

    private Task<List<StagingDatabase>> RunAsync(
        EtlPipelineContext context,
        params SnapshotFile[] inputs) =>
        StageRunner.RunAsync(
            new LoadDuckDbStage(_storage, _writer, NullLogger<LoadDuckDbStage>.Instance),
            inputs,
            context);

    private SnapshotFile Snapshot(string dataSet, string timestamp, string content = "parquet")
    {
        var key = $"{dataSet}/{dataSet}_{timestamp}.parquet";
        Snapshots.Put(key, content);

        return new SnapshotFile(new DataSetDefinition(dataSet, $"{dataSet}_{{0}}", ["CPH"], ChangeType.HeaderName, []))
        {
            Key = key,
            SourceTimestamp = DateTimeOffset.ParseExact(
                timestamp, EtlConstants.DateTimePattern, null, System.Globalization.DateTimeStyles.AssumeUniversal)
        };
    }

    [Fact]
    public async Task Collapses_all_snapshots_into_a_single_database()
    {
        var output = await RunAsync(
            Snapshot("sam_cph_holdings", "20251115121333"),
            Snapshot("cts_keeper", "20251114121333"));

        output.Should().ContainSingle();
        Staging.Keys.Should().ContainSingle();
        _writer.Sources.Select(source => source.TableName)
            .Should().Equal("sam_cph_holdings", "cts_keeper");
    }

    [Fact]
    public async Task Names_the_database_after_the_newest_source_timestamp_loaded()
    {
        var output = await RunAsync(
            Snapshot("sam_cph_holdings", "20251115121333"),
            Snapshot("cts_keeper", "20251114121333"));

        output.Single().Key.Should().Be("keeper_data_bridge_20251115121333.duckdb");
        output.Single().SourceTimestamp.Should().Be(
            new DateTimeOffset(2025, 11, 15, 12, 13, 33, TimeSpan.Zero));
        Staging.Keys.Should().Equal("keeper_data_bridge_20251115121333.duckdb");
    }

    [Fact]
    public async Task Produces_no_database_for_an_empty_input()
    {
        var output = await RunAsync();

        output.Should().BeEmpty();
        Staging.Keys.Should().BeEmpty();
        _writer.Calls.Should().Be(0);
    }

    [Fact]
    public async Task Dataset_filtered_run_does_not_publish_a_partial_shared_database()
    {
        var output = await RunAsync(
            StageRunner.Context(dataset: "sam_cph_holdings"),
            Snapshot("sam_cph_holdings", "20251115121333"));

        output.Should().BeEmpty();
        Staging.Keys.Should().BeEmpty();
        _writer.Calls.Should().Be(0);
    }

    [Fact]
    public async Task Reuses_an_existing_database_for_the_same_snapshots()
    {
        Staging.Put("keeper_data_bridge_20251115121333.duckdb", "already here");

        var output = await RunAsync(Snapshot("sam_cph_holdings", "20251115121333"));

        output.Single().Created.Should().BeFalse();
        _writer.Calls.Should().Be(0);
        Staging.ContentOf("keeper_data_bridge_20251115121333.duckdb").Should().Be("already here");
    }

    [Fact]
    public async Task Reports_the_tables_the_writer_created()
    {
        _writer.RowCount = 3;

        var output = await RunAsync(Snapshot("sam_cph_holdings", "20251115121333"));

        output.Single().Created.Should().BeTrue();
        output.Single().Tables.Should().ContainSingle()
            .Which.Should().BeEquivalentTo(new StagingTable(
                "sam_cph_holdings", "sam_cph_holdings/sam_cph_holdings_20251115121333.parquet", 3));
    }

    [Fact]
    public async Task Hands_the_writer_the_snapshot_content_on_local_disk()
    {
        await RunAsync(Snapshot("sam_cph_holdings", "20251115121333", content: "the snapshot bytes"));

        _writer.ContentWritten.Should().Equal("the snapshot bytes");
    }

    [Fact]
    public async Task Publishes_nothing_when_the_writer_fails()
    {
        _writer.Fail = true;

        var act = () => RunAsync(Snapshot("sam_cph_holdings", "20251115121333"));

        await act.Should().ThrowAsync<InvalidOperationException>();
        Staging.Keys.Should().BeEmpty();
    }

    /// <summary>Stands in for the DuckDB writer: records what it was asked to load, and reads each
    /// Parquet path so the test can prove the stage put the snapshot on disk for it.</summary>
    private sealed class RecordingStagingDatabaseWriter : IStagingDatabaseWriter
    {
        public List<StagingTableSource> Sources { get; } = [];

        public List<string> ContentWritten { get; } = [];

        public int Calls { get; private set; }

        public long RowCount { get; set; }

        public bool Fail { get; set; }

        public async Task<StagingDatabaseWriteResult> WriteAsync(
            IReadOnlyList<StagingTableSource> sources,
            string databasePath,
            CancellationToken cancellationToken = default)
        {
            Calls++;
            Sources.AddRange(sources);

            foreach (var source in sources)
            {
                ContentWritten.Add(await System.IO.File.ReadAllTextAsync(source.ParquetPath, cancellationToken));
            }

            if (Fail)
            {
                throw new InvalidOperationException("writer failed");
            }

            await System.IO.File.WriteAllTextAsync(databasePath, "duckdb", Encoding.UTF8, cancellationToken);

            return new StagingDatabaseWriteResult(
                [.. sources.Select(source => new StagingTable(source.TableName, source.SnapshotKey, RowCount))]);
        }
    }
}
