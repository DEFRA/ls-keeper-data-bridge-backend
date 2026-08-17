using DuckDB.NET.Data;
using FluentAssertions;
using FluentAssertions.Execution;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Staging;
using KeeperData.Core.EtlPipeline.Stages;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Core.Pipeline;
using KeeperData.Infrastructure.EtlPipeline.Staging;
using KeeperData.Infrastructure.Tests.Unit.EtlPipeline.EndToEnd.Harness;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.EndToEnd;

/// <summary>
/// End-to-end coverage of the ETL pipeline with no docker and no network:
/// source -> discover -> decrypt -> normalise -> snapshot -> load.
///
/// Real crypto, real Parquet, real delta merge, real executor, real stage graph. Only blob storage
/// is substituted, plus the staging database writer in all but one test. Deliberately carries no
/// "Dependence" trait, so it runs in the per-commit CI job alongside the unit tests.
///
/// The LocalStack suite in KeeperData.Bridge.Tests.Integration still covers the S3 wiring these
/// tests replace, and remains the nightly gate.
/// </summary>
public sealed class EtlPipelineEndToEndCiTests
{
    private static readonly string CphSnapshotKey =
        SnapshotFileNaming.SnapshotKey(EtlFixtures.CphHolding, EtlFixtures.LatestSourceTimestamp);

    private static readonly string HerdSnapshotKey =
        SnapshotFileNaming.SnapshotKey(EtlFixtures.Herd, EtlFixtures.LatestSourceTimestamp);

    private static readonly string PartySnapshotKey =
        SnapshotFileNaming.SnapshotKey(EtlFixtures.Party, EtlFixtures.LatestSourceTimestamp);

    [Fact]
    public async Task Pipeline_MaterialisesEveryFolder_ForEveryDataset()
    {
        using var host = CreateHost();
        await SeedAllThreeAsync(host);

        await host.RunAsync();

        host.Folders.Folder(EtlPipelineFolders.Raw).Keys.Should().HaveCount(9,
            "three dated files for each of the three datasets are decrypted into raw/ under their own names");

        host.Folders.Folder(EtlPipelineFolders.Normalised).Keys.Should().BeEquivalentTo(
        [
            $"sam_cph_holdings/{Path.GetFileNameWithoutExtension(EtlFixtures.FileName(EtlFixtures.CphHolding, EtlFixtures.FirstTimestamp))}.parquet",
            $"sam_cph_holdings/{Path.GetFileNameWithoutExtension(EtlFixtures.FileName(EtlFixtures.CphHolding, EtlFixtures.SecondTimestamp))}.parquet",
            $"sam_cph_holdings/{Path.GetFileNameWithoutExtension(EtlFixtures.FileName(EtlFixtures.CphHolding, EtlFixtures.ThirdTimestamp))}.parquet",
            $"sam_herd/{Path.GetFileNameWithoutExtension(EtlFixtures.FileName(EtlFixtures.Herd, EtlFixtures.FirstTimestamp))}.parquet",
            $"sam_herd/{Path.GetFileNameWithoutExtension(EtlFixtures.FileName(EtlFixtures.Herd, EtlFixtures.SecondTimestamp))}.parquet",
            $"sam_herd/{Path.GetFileNameWithoutExtension(EtlFixtures.FileName(EtlFixtures.Herd, EtlFixtures.ThirdTimestamp))}.parquet",
            $"sam_party/{Path.GetFileNameWithoutExtension(EtlFixtures.FileName(EtlFixtures.Party, EtlFixtures.FirstTimestamp))}.parquet",
            $"sam_party/{Path.GetFileNameWithoutExtension(EtlFixtures.FileName(EtlFixtures.Party, EtlFixtures.SecondTimestamp))}.parquet",
            $"sam_party/{Path.GetFileNameWithoutExtension(EtlFixtures.FileName(EtlFixtures.Party, EtlFixtures.ThirdTimestamp))}.parquet"
        ], "each dataset's parquet files are kept together under its own prefix");

        host.Folders.Folder(EtlPipelineFolders.Snapshots).Keys.Should().Contain(
            [CphSnapshotKey, HerdSnapshotKey, PartySnapshotKey],
            "each dataset gets a snapshot at the latest source timestamp it includes");

        host.Folders.Folder(EtlPipelineFolders.Staging).Keys.Should().ContainSingle()
            .Which.Should().Be(StagingFileNaming.DatabaseKey(EtlFixtures.LatestSourceTimestamp),
                "one staging database is produced, named for the newest source timestamp in the run");
    }

    [Fact]
    public async Task RawFolder_HoldsThePlaintextOfTheEncryptedSource()
    {
        using var host = CreateHost();
        await SeedAllThreeAsync(host);

        await host.RunAsync();

        var fileName = EtlFixtures.FileName(EtlFixtures.Party, EtlFixtures.ThirdTimestamp);
        var expected = EtlFixtures.FilesFor(EtlFixtures.Party).Single(file => file.FileName == fileName).Content;

        host.Folders.Folder(EtlPipelineFolders.Raw).TextOf(fileName).Should().Be(expected,
            "the decrypt stage derives the password from the object key, so the round trip must be exact");
    }

    [Theory]
    [InlineData("cph")]
    [InlineData("herd")]
    [InlineData("party")]
    public async Task Snapshot_MatchesTheDatasetsIngestionMode(string dataset)
    {
        using var host = CreateHost();
        await SeedAllThreeAsync(host);

        await host.RunAsync();

        var (key, keyColumn, valueColumn, expected) = ExpectationFor(dataset);

        var rows = await SnapshotReader.ReadColumnsAsync(host, key, keyColumn, valueColumn);

        rows.Select(row => (row[0], row[1]))
            .Should().BeEquivalentTo(expected,
                "Delta mode folds every file onto the snapshot, Snapshot mode copies only the newest");
    }

    [Fact]
    public async Task Snapshot_IgnoresDeleteRows_WhichIsCurrentDesign()
    {
        using var host = CreateHost([EtlFixtures.CphHolding]);
        await SeedAsync(host, EtlFixtures.CphHolding);

        await host.RunAsync();

        var rows = await SnapshotReader.ReadColumnsAsync(host, CphSnapshotKey, "CPH", "HOLDING_NAME");

        rows.Select(row => row[0]).Should().Contain(EtlFixtures.CphDeletedKey,
            "MergeState counts a D row and skips it, so the row it names survives at its previous value");

        rows.Single(row => row[0] == EtlFixtures.CphDeletedKey)[1].Should().Be("Keep Farm",
            "an ignored delete must not partially apply either");
    }

    [Fact]
    public async Task Snapshot_ForASnapshotModeDataset_IsTheNewestFileOnly()
    {
        using var host = CreateHost([EtlFixtures.Party]);
        await SeedAsync(host, EtlFixtures.Party);

        await host.RunAsync();

        var rows = await SnapshotReader.ReadColumnsAsync(host, PartySnapshotKey, "PARTY_ID");

        rows.Select(row => row[0]).Should().NotContain("P0000004",
            "a key present only in an earlier file is gone, because Snapshot mode copies rather than merges");
    }

    [Fact]
    public async Task Snapshot_DropsTheChangeTypeColumn()
    {
        using var host = CreateHost();
        await SeedAllThreeAsync(host);

        await host.RunAsync();

        var columns = await SnapshotReader.ColumnNamesAsync(host, CphSnapshotKey);

        columns.Should().NotContain(ChangeType.HeaderName,
            "change type describes how a row got here, not the current state, so it does not survive the merge");
        columns.Should().Contain("CPH", "the key columns do survive");
    }

    [Fact]
    public async Task DatasetFilter_RunsOnlyTheNamedDataset()
    {
        using var host = CreateHost();
        await SeedAllThreeAsync(host);

        await host.RunAsync(dataset: EtlFixtures.Herd.Name);

        host.Folders.Folder(EtlPipelineFolders.Snapshots).Keys.Should().BeEquivalentTo([HerdSnapshotKey],
            "the dataset filter is applied at the source stage, so nothing else is even discovered");

        host.Folders.Folder(EtlPipelineFolders.Raw).Keys.Should().HaveCount(3,
            "only the named dataset's files are decrypted");
    }

    [Fact]
    public async Task SecondRun_WithNothingNew_ProducesNothingFurther()
    {
        using var host = CreateHost();
        await SeedAllThreeAsync(host);

        await host.RunAsync();

        var afterFirst = FolderStateOf(host);

        await host.RunAsync();

        FolderStateOf(host).Should().BeEquivalentTo(afterFirst,
            "a re-run with no new source file is a no-op: every stage finds its output already present");
    }

    [Fact]
    public async Task SourceFile_EncryptedWithTheWrongSalt_FailsWithADiagnosableException()
    {
        using var host = CreateHost([EtlFixtures.Party]);

        foreach (var (fileName, content) in EtlFixtures.FilesFor(EtlFixtures.Party))
        {
            await host.PutEncryptedSourceFileAsync(fileName, content, salt: "a-salt-from-another-environment");
        }

        var run = async () => await host.RunAsync();

        (await run.Should().ThrowAsync<PipelineExecutionException>())
            .WithInnerException<SourceFileDecryptionException>(
                "a wrong key looks like a padding error, and the pipeline must say what it actually is");
    }

    [Fact]
    public async Task OneFailingDataset_AbortsTheWholeRun()
    {
        using var host = CreateHost();
        await SeedAllThreeAsync(host);

        // Only sam_party is unreadable. The executor drains each stage across every dataset before
        // the next stage runs, so there is no per-dataset isolation: the run fails as a whole.
        await host.PutEncryptedSourceFileAsync(
            EtlFixtures.FileName(EtlFixtures.Party, EtlFixtures.ThirdTimestamp),
            EtlFixtures.FilesFor(EtlFixtures.Party)[2].Content,
            salt: "a-salt-from-another-environment");

        var run = async () => await host.RunAsync();

        await run.Should().ThrowAsync<PipelineExecutionException>();

        host.Folders.Folder(EtlPipelineFolders.Staging).Keys.Should().BeEmpty(
            "no staging database is published when any dataset in the run failed");
    }

    [Fact]
    public async Task SourceFile_WithoutAParsableTimestamp_FailsTheRun()
    {
        using var host = CreateHost([EtlFixtures.Party]);

        // Guards the trap the local kickstart script works around: the Crypto tool names its delta
        // file "..._delta.csv", and the timestamp is read from the segment after the LAST underscore.
        await host.PutEncryptedSourceFileAsync(
            "LITP_SAMPARTY_20251113121333_delta.csv",
            EtlFixtures.FilesFor(EtlFixtures.Party)[0].Content);

        var run = async () => await host.RunAsync();

        await run.Should().ThrowAsync<PipelineExecutionException>();
    }

    [Fact]
    public async Task LoadStage_WritesTheSnapshotsIntoTheStagingDatabase()
    {
        var writer = new RecordingStagingDatabaseWriter();

        using var host = CreateHost(EtlFixtures.AllThree, writer);
        await SeedAllThreeAsync(host);

        await host.RunAsync();

        writer.OnlyCall.Select(source => source.TableName).Should().BeEquivalentTo(
            [EtlFixtures.CphHolding.Name, EtlFixtures.Herd.Name, EtlFixtures.Party.Name],
            "one table per dataset, named for the dataset");

        writer.OnlyCall.Select(source => source.SnapshotKey).Should().BeEquivalentTo(
            [CphSnapshotKey, HerdSnapshotKey, PartySnapshotKey],
            "each table is built from that dataset's latest snapshot");
    }

    [Fact]
    public async Task StagingDatabase_IsQueryable_WithTheRealDuckDbWriter()
    {
        // The one test that runs the real writer, so the generated SQL is covered too. DuckDB is an
        // in-process native library, so this still needs no docker.
        using var host = CreateHost(EtlFixtures.AllThree, new DuckDbStagingDatabaseWriter(new NullLogger<DuckDbStagingDatabaseWriter>()));
        await SeedAllThreeAsync(host);

        await host.RunAsync();

        var databaseKey = StagingFileNaming.DatabaseKey(EtlFixtures.LatestSourceTimestamp);
        var path = await host.DownloadToTempAsync(EtlPipelineFolders.Staging, databaseKey, ".duckdb");

        try
        {
            using var connection = new DuckDBConnection($"Data Source={path}");
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            command.CommandText = "SELECT PARTY_ID, PARTY_NAME FROM sam_party ORDER BY PARTY_ID";

            using var reader = await command.ExecuteReaderAsync();

            var rows = new List<(string, string)>();
            while (await reader.ReadAsync())
            {
                rows.Add((reader.GetString(0), reader.GetString(1)));
            }

            rows.Should().BeEquivalentTo(EtlFixtures.ExpectedParty,
                "what lands in the database is what the snapshot said, with no rows lost in the load");
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task HappyPath_FromEncryptedSourceToQueryableDatabase()
    {
        // The one test that walks the whole chain in order for all three datasets, so a failure says
        // "the pipeline is broken" rather than "a stage is broken". The focused tests above stay,
        // because when this goes red they are what tells you which stage did it.
        using var host = CreateHost(
            EtlFixtures.AllThree,
            new DuckDbStagingDatabaseWriter(new NullLogger<DuckDbStagingDatabaseWriter>()));

        await SeedAllThreeAsync(host);

        await host.RunAsync();

        using var scope = new AssertionScope();

        // 1. Decrypted. The plaintext in raw/ is byte-for-byte what was encrypted into the source.
        foreach (var definition in EtlFixtures.AllThree)
        {
            foreach (var (fileName, content) in EtlFixtures.FilesFor(definition))
            {
                host.Folders.Folder(EtlPipelineFolders.Raw).TextOf(fileName).Should().Be(content,
                    "raw/{0} must round-trip through AES with the object key as its password", fileName);
            }
        }

        // 2. Normalised. Three Parquet files per dataset, each under its own prefix.
        foreach (var definition in EtlFixtures.AllThree)
        {
            host.Folders.Folder(EtlPipelineFolders.Normalised).Keys
                .Where(key => key.StartsWith(SnapshotFileNaming.DataSetPrefix(definition), StringComparison.Ordinal))
                .Should().HaveCount(3, "every source file for {0} is normalised", definition.Name);
        }

        // 3. Snapshotted. The rows are what that dataset's ingestion mode says they should be.
        (await SnapshotReader.ReadColumnsAsync(host, CphSnapshotKey, "CPH", "HOLDING_NAME"))
            .Select(row => (row[0], row[1]))
            .Should().BeEquivalentTo(EtlFixtures.ExpectedCph.Select(row => (row.Cph, row.HoldingName)),
                "sam_cph_holdings is Delta mode, so the three files fold together");

        (await SnapshotReader.ReadColumnsAsync(host, HerdSnapshotKey, "CPHH", "HERD_NAME"))
            .Select(row => (row[0], row[1]))
            .Should().BeEquivalentTo(EtlFixtures.ExpectedHerd.Select(row => (row.Cphh, row.HerdName)),
                "sam_herd is Snapshot mode, so only the newest file survives");

        (await SnapshotReader.ReadColumnsAsync(host, PartySnapshotKey, "PARTY_ID", "PARTY_NAME"))
            .Select(row => (row[0], row[1]))
            .Should().BeEquivalentTo(EtlFixtures.ExpectedParty.Select(row => (row.PartyId, row.PartyName)),
                "sam_party is Snapshot mode with a single-column key");

        // 4. Loaded. All three tables are in one database and every one is queryable.
        var databaseKey = StagingFileNaming.DatabaseKey(EtlFixtures.LatestSourceTimestamp);
        var path = await host.DownloadToTempAsync(EtlPipelineFolders.Staging, databaseKey, ".duckdb");

        try
        {
            using var connection = new DuckDBConnection($"Data Source={path}");
            await connection.OpenAsync();

            (await QueryPairsAsync(connection, "SELECT CPH, HOLDING_NAME FROM sam_cph_holdings"))
                .Should().BeEquivalentTo(EtlFixtures.ExpectedCph.Select(row => (row.Cph, row.HoldingName)),
                    "the load stage must not lose or alter a row on its way into sam_cph_holdings");

            (await QueryPairsAsync(connection, "SELECT CPHH, HERD_NAME FROM sam_herd"))
                .Should().BeEquivalentTo(EtlFixtures.ExpectedHerd.Select(row => (row.Cphh, row.HerdName)),
                    "the load stage must not lose or alter a row on its way into sam_herd");

            (await QueryPairsAsync(connection, "SELECT PARTY_ID, PARTY_NAME FROM sam_party"))
                .Should().BeEquivalentTo(EtlFixtures.ExpectedParty.Select(row => (row.PartyId, row.PartyName)),
                    "the load stage must not lose or alter a row on its way into sam_party");
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static async Task<List<(string, string)>> QueryPairsAsync(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;

        using var reader = await command.ExecuteReaderAsync();

        var rows = new List<(string, string)>();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetString(0), reader.GetString(1)));
        }

        return rows;
    }

    private static InMemoryEtlPipelineHost CreateHost(
        IReadOnlyList<DataSetDefinition>? definitions = null,
        IStagingDatabaseWriter? writer = null)
        => InMemoryEtlPipelineHost.Create(EtlFixtures.RunClock, definitions ?? EtlFixtures.AllThree, writer);

    private static async Task SeedAllThreeAsync(InMemoryEtlPipelineHost host)
    {
        foreach (var definition in EtlFixtures.AllThree)
        {
            await SeedAsync(host, definition);
        }
    }

    private static async Task SeedAsync(InMemoryEtlPipelineHost host, DataSetDefinition definition)
    {
        foreach (var (fileName, content) in EtlFixtures.FilesFor(definition))
        {
            await host.PutEncryptedSourceFileAsync(fileName, content);
        }
    }

    private static Dictionary<string, IReadOnlyList<string>> FolderStateOf(InMemoryEtlPipelineHost host)
    {
        string[] folders =
        [
            EtlPipelineFolders.Raw,
            EtlPipelineFolders.Normalised,
            EtlPipelineFolders.Snapshots,
            EtlPipelineFolders.Staging
        ];

        return folders.ToDictionary(folder => folder, folder => host.Folders.Folder(folder).Keys);
    }

    private static (string SnapshotKey, string KeyColumn, string ValueColumn, IEnumerable<(string, string)> Expected)
        ExpectationFor(string dataset) => dataset switch
        {
            "cph" => (CphSnapshotKey, "CPH", "HOLDING_NAME",
                EtlFixtures.ExpectedCph.Select(row => (row.Cph, row.HoldingName))),
            "herd" => (HerdSnapshotKey, "CPHH", "HERD_NAME",
                EtlFixtures.ExpectedHerd.Select(row => (row.Cphh, row.HerdName))),
            "party" => (PartySnapshotKey, "PARTY_ID", "PARTY_NAME",
                EtlFixtures.ExpectedParty.Select(row => (row.PartyId, row.PartyName))),
            _ => throw new ArgumentOutOfRangeException(nameof(dataset), dataset, "No expectation for this dataset.")
        };
}
