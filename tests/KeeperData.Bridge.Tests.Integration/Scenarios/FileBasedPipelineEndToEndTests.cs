using System.Text;
using DuckDB.NET.Data;
using FluentAssertions;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Staging;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Pipeline;
using Parquet;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Scenarios;

/// <summary>
/// End-to-end coverage of the ETL pipeline: source -> discover -> decrypt -> normalise
/// -> snapshot -> load-duckdb, over LocalStack S3 with real crypto, real Parquet and real DuckDB.
///
/// The legacy Mongo ETL is not wired in here and is never reached; these tests are about the new
/// pipeline alone. Every folder it materialises is asserted, not just the database at the end,
/// because the value of an E2E test is catching the stage that quietly produced nothing.
///
/// The fixture is the one from the parent ticket: three deltas whose net effect is one update, one
/// untouched row, one insert and one ignored delete.
/// </summary>
[Collection("LocalStack"), Trait("Dependence", "docker")]
public sealed class EtlPipelineEndToEndTests(ITestOutputHelper output, LocalStackFixture localStack)
{
    // The dataset's real composite key is CPH + FEATURE_NAME + SECONDARY_CPH + ANIMAL_SPECIES_CODE,
    // so the fixture carries all four; only CPH varies between rows.
    private const string Header = "CPH|FEATURE_NAME|SECONDARY_CPH|ANIMAL_SPECIES_CODE|HOLDING_NAME|CHANGE_TYPE";

    private const string KeyColumns = "MAIN|-|01";

    private const string FirstDelta = "LITP_SAMCPHHOLDING_20251113121333.csv";
    private const string SecondDelta = "LITP_SAMCPHHOLDING_20251114121333.csv";
    private const string ThirdDelta = "LITP_SAMCPHHOLDING_20251115121333.csv";

    private static readonly DateTimeOffset RunClock = new(2025, 11, 15, 18, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Pipeline_WalksTheDeltas_AndMaterialisesEveryFolder()
    {
        await using var host = await CreateHostAsync();
        await SeedTheThreeDeltasAsync(host);

        await host.RunPipelineAsync();

        (await host.ListFolderAsync(EtlPipelineFolders.Raw)).Should().BeEquivalentTo(
            [FirstDelta, SecondDelta, ThirdDelta],
            "every discovered source file is decrypted into raw/ under its own name");

        (await host.ReadTextAsync(EtlPipelineFolders.Raw, ThirdDelta))
            .Should().Be(ThirdDeltaContent(), "raw/ holds the plaintext of the encrypted source file");

        (await host.ListFolderAsync(EtlPipelineFolders.Normalised)).Should().BeEquivalentTo(
            [
                "sam_cph_holdings/LITP_SAMCPHHOLDING_20251113121333.parquet",
                "sam_cph_holdings/LITP_SAMCPHHOLDING_20251114121333.parquet",
                "sam_cph_holdings/LITP_SAMCPHHOLDING_20251115121333.parquet"
            ],
            "each raw file becomes one Parquet file, keeping its source timestamp");

        var snapshots = await host.ListFolderAsync(EtlPipelineFolders.Snapshots);
        snapshots.Should().ContainSingle().Which.Should().Be(
            "sam_cph_holdings/sam_cph_holdings_20251115121333.parquet",
            "the snapshot is named with the newest source timestamp it includes, not the run time");

        var snapshotRows = await ReadSnapshotAsync(host, snapshots[0]);
        snapshotRows.Should().BeEquivalentTo(ExpectedCurrentState(),
            "I and U upsert, D is ignored, and later rows win");

        var staging = await host.ListFolderAsync(EtlPipelineFolders.Staging);
        staging.Should().ContainSingle().Which.Should().Be(
            "keeper_data_bridge_20251115121333.duckdb",
            "the database carries the same source timestamp as the snapshot it holds");

        var rows = await QueryDatabaseAsync(host, staging[0]);
        rows.Should().BeEquivalentTo(ExpectedCurrentState(),
            "the database is the snapshot, loaded into a table named after the dataset");

        output.WriteLine($"Snapshot and database agree on {rows.Count} row(s)");
    }

    [Fact]
    public async Task SnapshotTable_DropsChangeType_AndIsIndependentlyQueryable()
    {
        await using var host = await CreateHostAsync();
        await SeedTheThreeDeltasAsync(host);

        await host.RunPipelineAsync();

        var databasePath = await host.DownloadToTempAsync(
            EtlPipelineFolders.Staging, "keeper_data_bridge_20251115121333.duckdb", ".duckdb");

        try
        {
            // Opened as its own file, with nothing from the pipeline attached: this is what someone
            // downloading through the presigned URL gets.
            using var connection = new DuckDBConnection($"Data Source={databasePath}");
            await connection.OpenAsync();

            var columns = new List<string>();
            using (var describe = connection.CreateCommand())
            {
                describe.CommandText = "DESCRIBE sam_cph_holdings";
                using var reader = await describe.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    columns.Add(reader.GetString(0));
                }
            }

            columns.Should().BeEquivalentTo(
                ["CPH", "FEATURE_NAME", "SECONDARY_CPH", "ANIMAL_SPECIES_CODE", "HOLDING_NAME"],
                "CHANGE_TYPE is a delta instruction, not part of the current state");
        }
        finally
        {
            File.Delete(databasePath);
        }
    }

    [Fact]
    public async Task SecondRun_WithANewDelta_ResumesFromTheLatestSnapshot()
    {
        await using var host = await CreateHostAsync();
        await SeedTheThreeDeltasAsync(host);

        await host.RunPipelineAsync();

        const string fourthDelta = "LITP_SAMCPHHOLDING_20251116121333.csv";
        await host.PutEncryptedSourceFileAsync(fourthDelta, Psv(
            ("01/001/0003", "Renamed Farm", "U"),
            ("01/001/0004", "Fourth Farm", "I")));

        host.SetNow(RunClock.AddDays(1));

        await host.RunPipelineAsync();

        (await host.ListFolderAsync(EtlPipelineFolders.Raw)).Should().HaveCount(4,
            "the three files already in raw/ are skipped rather than decrypted again");

        (await host.ListFolderAsync(EtlPipelineFolders.Snapshots)).Should().BeEquivalentTo(
            [
                "sam_cph_holdings/sam_cph_holdings_20251115121333.parquet",
                "sam_cph_holdings/sam_cph_holdings_20251116121333.parquet"
            ],
            "the earlier snapshot is retained; the new one is a sibling, not a replacement");

        var rows = await ReadSnapshotAsync(host, "sam_cph_holdings/sam_cph_holdings_20251116121333.parquet");
        rows.Should().BeEquivalentTo(new[]
        {
            ("01/001/0001", "Updated Farm"),
            ("01/001/0002", "Keep Farm"),
            ("01/001/0003", "Renamed Farm"),
            ("01/001/0004", "Fourth Farm")
        }, "only the new delta is applied, onto the state the previous snapshot already held");

        (await host.ListFolderAsync(EtlPipelineFolders.Staging)).Should().BeEquivalentTo(
            ["keeper_data_bridge_20251115121333.duckdb", "keeper_data_bridge_20251116121333.duckdb"]);

        (await QueryDatabaseAsync(host, "keeper_data_bridge_20251116121333.duckdb")).Should().HaveCount(4);
    }

    [Fact]
    public async Task ReRun_WithNothingNew_ProducesNothingFurther()
    {
        await using var host = await CreateHostAsync();
        await SeedTheThreeDeltasAsync(host);

        await host.RunPipelineAsync();

        var afterFirstRun = await SnapshotOfEveryFolderAsync(host);

        await host.RunPipelineAsync();

        (await SnapshotOfEveryFolderAsync(host)).Should().BeEquivalentTo(afterFirstRun,
            "a run with no new source file is a no-op in every folder");
    }

    [Fact]
    public async Task Delta_OlderThanTheLatestSnapshot_IsNotApplied()
    {
        await using var host = await CreateHostAsync();
        await SeedTheThreeDeltasAsync(host);

        await host.RunPipelineAsync();

        // A file that should have arrived before the snapshot was taken, turning up late.
        await host.PutEncryptedSourceFileAsync(
            "LITP_SAMCPHHOLDING_20251114181333.csv",
            Psv(("01/001/0009", "Late Farm", "I")));

        await host.RunPipelineAsync();

        (await host.ListFolderAsync(EtlPipelineFolders.Normalised)).Should().HaveCount(4,
            "a late file is still decrypted and normalised");

        (await host.ListFolderAsync(EtlPipelineFolders.Snapshots)).Should().ContainSingle()
            .Which.Should().Be("sam_cph_holdings/sam_cph_holdings_20251115121333.parquet",
                "the snapshot only walks forward, so nothing older than it is applied and no snapshot is written");

        var rows = await ReadSnapshotAsync(host, "sam_cph_holdings/sam_cph_holdings_20251115121333.parquet");
        rows.Should().NotContain(row => row.Cph == "01/001/0009",
            "the late row is silently dropped - the behaviour to be aware of, not necessarily the one we want");
    }

    [Fact]
    public async Task SourceFile_WithoutAParsableTimestamp_FailsTheRun()
    {
        await using var host = await CreateHostAsync();
        await SeedTheThreeDeltasAsync(host);

        await host.PutEncryptedSourceFileAsync(
            "LITP_SAMCPHHOLDING_NOTATIMESTAMP.csv",
            Psv(("01/001/0005", "Nameless Farm", "I")));

        var run = async () => await host.RunPipelineAsync();

        var failure = await run.Should().ThrowAsync<PipelineExecutionException>();
        failure.WithInnerException<InvalidOperationException>()
            .WithMessage("*timestamp*",
                "ordering comes from the file name alone, so a file that cannot be placed in the sequence fails the run rather than being guessed at");

        (await host.ListFolderAsync(EtlPipelineFolders.Snapshots)).Should().BeEmpty();
        (await host.ListFolderAsync(EtlPipelineFolders.Staging)).Should().BeEmpty();
    }

    [Fact]
    public async Task LoadFailure_LeavesStagingEmpty()
    {
        await using var host = await CreateHostAsync(new ThrowingStagingDatabaseWriter());
        await SeedTheThreeDeltasAsync(host);

        var run = async () => await host.RunPipelineAsync();

        await run.Should().ThrowAsync<PipelineExecutionException>();

        (await host.ListFolderAsync(EtlPipelineFolders.Snapshots)).Should().ContainSingle(
            "the stages before the failure keep what they produced");

        (await host.ListFolderAsync(EtlPipelineFolders.Staging)).Should().BeEmpty(
            "a database is uploaded only once it is complete, so a partial load publishes nothing");
    }

    [Fact]
    public async Task Pipeline_HandlesAFileOfRealisticSize()
    {
        const int rowCount = 25_000;

        await using var host = await CreateHostAsync();

        var builder = new StringBuilder(Header).AppendLine();
        for (var i = 0; i < rowCount; i++)
        {
            builder.Append("01/001/").Append(i.ToString("D5")).Append('|').Append(KeyColumns)
                .Append("|Farm ").Append(i).AppendLine("|I");
        }

        await host.PutEncryptedSourceFileAsync(FirstDelta, builder.ToString());

        await host.RunPipelineAsync();

        var rows = await QueryDatabaseAsync(host, "keeper_data_bridge_20251113121333.duckdb");
        rows.Should().HaveCount(rowCount, "no rows are lost between the source file and the database");
    }

    private Task<EtlPipelineTestHost> CreateHostAsync(IStagingDatabaseWriter? writer = null)
        => EtlPipelineTestHost.CreateAsync(localStack.S3Client, RunClock, stagingDatabaseWriter: writer);

    private static async Task SeedTheThreeDeltasAsync(EtlPipelineTestHost host)
    {
        await host.PutEncryptedSourceFileAsync(FirstDelta, Psv(
            ("01/001/0001", "Old Farm", "I"),
            ("01/001/0002", "Keep Farm", "I")));

        await host.PutEncryptedSourceFileAsync(SecondDelta, Psv(
            ("01/001/0001", "Updated Farm", "U"),
            ("01/001/0003", "New Farm", "I")));

        await host.PutEncryptedSourceFileAsync(ThirdDelta, ThirdDeltaContent());
    }

    private static string ThirdDeltaContent()
        => Psv(("01/001/0002", "Keep Farm", "D"));

    private static (string Cph, string Name)[] ExpectedCurrentState()
        =>
        [
            ("01/001/0001", "Updated Farm"),
            ("01/001/0002", "Keep Farm"),
            ("01/001/0003", "New Farm")
        ];

    private static string Psv(params (string Cph, string Name, string ChangeType)[] rows)
    {
        var builder = new StringBuilder(Header).AppendLine();

        foreach (var (cph, name, changeType) in rows)
        {
            builder.Append(cph).Append('|').Append(KeyColumns).Append('|').Append(name).Append('|').AppendLine(changeType);
        }

        return builder.ToString();
    }

    /// <summary>Reads a snapshot Parquet directly, so the assertion does not depend on the load stage.</summary>
    private static async Task<List<(string Cph, string Name)>> ReadSnapshotAsync(
        EtlPipelineTestHost host, string key)
    {
        var path = await host.DownloadToTempAsync(EtlPipelineFolders.Snapshots, key, ".parquet");

        try
        {
            await using var file = File.OpenRead(path);
            await using var reader = await ParquetReader.CreateAsync(file);

            var fields = reader.Schema.GetDataFields();
            var cph = Array.FindIndex(fields, field => field.Name == "CPH");
            var name = Array.FindIndex(fields, field => field.Name == "HOLDING_NAME");

            var rows = new List<(string, string)>();

            for (var group = 0; group < reader.RowGroupCount; group++)
            {
                using var rowGroup = reader.OpenRowGroupReader(group);

                var cphColumn = new string?[rowGroup.RowCount];
                var nameColumn = new string?[rowGroup.RowCount];

                await rowGroup.ReadAsync(fields[cph], cphColumn.AsMemory());
                await rowGroup.ReadAsync(fields[name], nameColumn.AsMemory());

                rows.AddRange(cphColumn.Select((value, index) => (value!, nameColumn[index]!)));
            }

            return rows;
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static async Task<List<(string Cph, string Name)>> QueryDatabaseAsync(
        EtlPipelineTestHost host, string key)
    {
        var path = await host.DownloadToTempAsync(EtlPipelineFolders.Staging, key, ".duckdb");

        try
        {
            using var connection = new DuckDBConnection($"Data Source={path}");
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            command.CommandText = "SELECT CPH, HOLDING_NAME FROM sam_cph_holdings ORDER BY CPH";

            using var reader = await command.ExecuteReaderAsync();

            var rows = new List<(string, string)>();
            while (await reader.ReadAsync())
            {
                rows.Add((reader.GetString(0), reader.GetString(1)));
            }

            return rows;
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static async Task<Dictionary<string, IReadOnlyList<string>>> SnapshotOfEveryFolderAsync(
        EtlPipelineTestHost host)
    {
        string[] folders =
        [
            EtlPipelineFolders.Raw,
            EtlPipelineFolders.Normalised,
            EtlPipelineFolders.Snapshots,
            EtlPipelineFolders.Staging
        ];

        var contents = new Dictionary<string, IReadOnlyList<string>>();

        foreach (var folder in folders)
        {
            contents[folder] = await host.ListFolderAsync(folder);
        }

        return contents;
    }

    private sealed class ThrowingStagingDatabaseWriter : IStagingDatabaseWriter
    {
        public Task<StagingDatabaseWriteResult> WriteAsync(
            IReadOnlyList<StagingTableSource> sources,
            string databasePath,
            CancellationToken cancellationToken = default)
            => throw new InvalidOperationException("Simulated failure part way through building the database");
    }
}
