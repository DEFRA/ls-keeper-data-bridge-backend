using DuckDB.NET.Data;
using FluentAssertions;
using FluentAssertions.Execution;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Staging;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Infrastructure.EtlPipeline.Staging;
using KeeperData.Infrastructure.Tests.Unit.EtlPipeline.EndToEnd.Harness;
using KeeperData.Tests.SharedFixtures;
using Microsoft.Extensions.Logging.Abstractions;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.EndToEnd;

/// <summary>
/// End-to-end coverage of the CTS location identifiers dataset, with no docker and no network:
/// encrypted source -> discover by glob -> decrypt with the derived password -> normalise -> fold or
/// reset the snapshot -> stage.
///
/// The two scenarios are the protocol the dataset is built around. Steady state: a snapshot carrying
/// the current bulk-set hash is folded onto, and a re-run with nothing new does nothing. Reset: a
/// bulk part appears, the hash moves, every bulk is reprocessed and only the deltas cut after them
/// replay - under a new key, so the previous lineage is still there.
///
/// Everything but blob storage and the two database writers is the production type, which is the
/// point: the password derivation, the glob, the hash in the snapshot name and the audit-aware merge
/// are only really wired up if a file encrypted the way the extract encrypts it comes out of the far
/// end as the right rows.
/// </summary>
public sealed class CtsLocationIdentifiersEndToEndCiTests
{
    private static readonly string SteadyHash = CtsFixtures.BaselineHashOf(CtsFixtures.BulkPartOneKey);

    private static readonly string ResetHash =
        CtsFixtures.BaselineHashOf(CtsFixtures.BulkPartOneKey, CtsFixtures.BulkPartTwoKey);

    /// <summary>Scenario A, run 1: bulk part 001 and the first four deltas.</summary>
    private static readonly string SteadySnapshotKey =
        CtsFixtures.SnapshotKeyFor(CtsFixtures.FourthDeltaSourceTimestamp, SteadyHash);

    /// <summary>Scenario A, run 3: the fifth delta moves the name on, in the same lineage.</summary>
    private static readonly string AdvancedSnapshotKey =
        CtsFixtures.SnapshotKeyFor(CtsFixtures.FifthDeltaSourceTimestamp, SteadyHash);

    /// <summary>Scenario B: the same timestamp as run 3, a different lineage.</summary>
    private static readonly string ResetSnapshotKey =
        CtsFixtures.SnapshotKeyFor(CtsFixtures.FifthDeltaSourceTimestamp, ResetHash);

    [Fact]
    public async Task RawFolder_HoldsThePlaintext_OfAFileEncryptedWithItsDerivedPassword()
    {
        using var host = CreateHost();
        await SeedSteadyAsync(host);

        await host.RunAsync();

        host.Folders.Folder(EtlPipelineFolders.Raw).TextOf(CtsFixtures.BulkPartOneKey)
            .Should().Be(CtsFixtures.BulkPartOne,
                "the extract encrypts a CTS file with a password derived from its name, not with the name");
    }

    [Fact]
    public async Task SteadyState_FoldsTheDeltasOntoTheBulk_AtTheNewestDeltaTimestamp()
    {
        using var host = CreateHost();
        await SeedSteadyAsync(host);

        await host.RunAsync();

        using var scope = new AssertionScope();

        host.Folders.Folder(EtlPipelineFolders.Snapshots).Keys.Should().BeEquivalentTo([SteadySnapshotKey],
            "the snapshot is named after the newest delta it includes and carries the bulk set's hash");

        (await RowsOf(host, SteadySnapshotKey)).Should().BeEquivalentTo(CtsFixtures.ExpectedSteady,
            "the updates are applied, the delete removes its key and the unrecognised audit type is rejected rather than guessed at");
    }

    /// <summary>Every row of both lanes carries RECORD_TYPE=D - it says "data record" - so a feed read
    /// through the wrong column would delete itself entirely.</summary>
    [Fact]
    public async Task SteadyState_ReadsTheChangeTypeFromTheAuditLane_NotFromTheRecordType()
    {
        using var host = CreateHost();
        await SeedSteadyAsync(host);

        await host.RunAsync();

        (await RowsOf(host, SteadySnapshotKey)).Should().HaveCount(CtsFixtures.ExpectedSteady.Length);
    }

    [Fact]
    public async Task Snapshot_CarriesTheBulkFilesDataShape_AndNothingElse()
    {
        using var host = CreateHost();
        await SeedSteadyAsync(host);

        await host.RunAsync();

        var columns = await SnapshotReader.ColumnNamesAsync(host, SteadySnapshotKey);

        columns.Should().BeEquivalentTo(CtsFixtures.SnapshotColumns,
            "the audit lane describes the change and the two counters are per-file, so none of them survive the merge");
    }

    [Fact]
    public async Task Snapshot_AppliesTheDelete_AndKeepsTheRowTheRejectedAuditTypeNames()
    {
        using var host = CreateHost();
        await SeedSteadyAsync(host);

        await host.RunAsync();

        var rows = await RowsOf(host, SteadySnapshotKey);

        using var scope = new AssertionScope();

        rows.Select(row => row.Id).Should().NotContain(CtsFixtures.DeletedKey,
            "a D row on an audit-aware dataset removes the key rather than being counted and skipped");

        rows.Single(row => row.Id == CtsFixtures.RejectedKey).Version.Should().Be("1",
            "the row the unrecognised audit type names stands as the previous delta inserted it: an unknown intent is not written");
    }

    [Fact]
    public async Task SecondRun_WithNothingNew_ProducesNothingFurther()
    {
        using var host = CreateHost();
        await SeedSteadyAsync(host);

        await host.RunAsync();

        var afterFirst = FolderStateOf(host);

        await host.RunAsync();

        FolderStateOf(host).Should().BeEquivalentTo(afterFirst,
            "the snapshot's name says which deltas it already holds, so a re-run finds nothing newer");
    }

    [Fact]
    public async Task FifthDelta_AdvancesTheSnapshot_WithinTheSameLineage()
    {
        using var host = CreateHost();
        await SeedSteadyAsync(host);

        await host.RunAsync();

        await PutAsync(host, CtsFixtures.FifthDeltaKey, CtsFixtures.FifthDelta);

        await host.RunAsync();

        using var scope = new AssertionScope();

        host.Folders.Folder(EtlPipelineFolders.Snapshots).Keys.Should().BeEquivalentTo(
            [SteadySnapshotKey, AdvancedSnapshotKey],
            "the bulk set has not moved, so the new delta folds onto the same lineage under a later name");

        (await RowsOf(host, AdvancedSnapshotKey)).Should().BeEquivalentTo(CtsFixtures.ExpectedSteadyAdvanced,
            "the twice-updated key takes the cut with the higher LID_AUD_ID, which the file carries second");
    }

    /// <summary>Two of the sample's five deltas carry nothing but a header. They still have to move the
    /// snapshot's name on, or every run reprocesses them forever.</summary>
    [Fact]
    public async Task HeaderOnlyDelta_MovesTheNameOn_WithoutChangingWhatTheSnapshotHolds()
    {
        using var host = CreateHost();

        await PutAsync(host, CtsFixtures.BulkPartOneKey, CtsFixtures.BulkPartOne);
        await PutAsync(host, CtsFixtures.FirstDeltaKey, CtsFixtures.FirstDelta);

        await host.RunAsync();

        await PutAsync(host, CtsFixtures.SecondDeltaKey, CtsFixtures.SecondDelta);

        await host.RunAsync();

        var advanced = CtsFixtures.SnapshotKeyFor(CtsFixtures.SecondDeltaSourceTimestamp, SteadyHash);

        using var scope = new AssertionScope();

        host.Folders.Folder(EtlPipelineFolders.Snapshots).Keys.Should().Contain(advanced);

        (await RowsOf(host, advanced)).Should().BeEquivalentTo(CtsFixtures.ExpectedBulkOnly,
            "a delta with no rows changes nothing but the name");
    }

    [Fact]
    public async Task ChangedBulkSet_RebuildsFromEveryBulk_AndKeepsThePreviousLineage()
    {
        using var host = CreateHost();
        await SeedSteadyAsync(host);
        await PutAsync(host, CtsFixtures.FifthDeltaKey, CtsFixtures.FifthDelta);

        await host.RunAsync();

        await PutAsync(host, CtsFixtures.BulkPartTwoKey, CtsFixtures.BulkPartTwo);

        await host.RunAsync();

        using var scope = new AssertionScope();

        host.Folders.Folder(EtlPipelineFolders.Snapshots).Keys.Should().BeEquivalentTo(
            [AdvancedSnapshotKey, ResetSnapshotKey],
            "the reset lands on the timestamp the lineage it replaces already carries, so only the hash tells them apart");

        (await RowsOf(host, ResetSnapshotKey)).Should().BeEquivalentTo(CtsFixtures.ExpectedReset,
            "both bulk parts are reprocessed and the deltas cut after them replay onto the rebuilt baseline");

        (await RowsOf(host, AdvancedSnapshotKey)).Should().BeEquivalentTo(CtsFixtures.ExpectedSteadyAdvanced,
            "and the snapshot the reset replaces is left exactly as it was");
    }

    /// <summary>The bulk parts of one cut share a timestamp, so the sample's own set cannot show this:
    /// the second part here is cut after the delta, which the rebuild must therefore discard rather
    /// than replay onto a baseline that already carries it.</summary>
    [Fact]
    public async Task ChangedBulkSet_DoesNotReplayTheDeltasCutBeforeIt()
    {
        using var host = CreateHost();

        await PutAsync(host, CtsFixtures.BulkPartOneKey, CtsFixtures.BulkPartOne);
        await PutAsync(host, CtsFixtures.ThirdDeltaKey, CtsFixtures.ThirdDelta);

        await host.RunAsync();

        await PutAsync(host, CtsFixtures.LateBulkPartTwoKey, CtsFixtures.BulkPartTwo);

        await host.RunAsync();

        var key = CtsFixtures.SnapshotKeyFor(
            CtsFixtures.LateBulkSourceTimestamp,
            CtsFixtures.BaselineHashOf(CtsFixtures.BulkPartOneKey, CtsFixtures.LateBulkPartTwoKey));

        using var scope = new AssertionScope();

        (await RowsOf(host, key)).Should().BeEquivalentTo(CtsFixtures.ExpectedBothBulksOnly,
            "a delta cut before the baseline is not replayed after it: the merge is last-writer-wins, so it would revert what the baseline already carries");

        host.Folders.Folder(EtlPipelineFolders.Snapshots).Keys.Should().Contain(
            CtsFixtures.SnapshotKeyFor(CtsFixtures.ThirdDeltaSourceTimestamp, SteadyHash),
            "and the lineage it rebuilt away from survives");
    }

    [Fact]
    public async Task StagingDatabase_CarriesTheDataset_WithTheSnapshotsRows()
    {
        using var host = CreateHost(new DuckDbStagingDatabaseWriter(new NullLogger<DuckDbStagingDatabaseWriter>()));
        await SeedSteadyAsync(host);

        await host.RunAsync();

        var databaseKey = StagingFileNaming.DatabaseKey(CtsFixtures.FourthDeltaSourceTimestamp);
        var path = await host.DownloadToTempAsync(EtlPipelineFolders.Staging, databaseKey, ".duckdb");

        try
        {
            using var connection = new DuckDBConnection($"Data Source={path}");
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            command.CommandText =
                "SELECT LID_ID, LID_FULL_IDENTIFIER, LID_CURRENT_MODIFIED_DATE, LID_VERSION " +
                "FROM cts_location_identifiers ORDER BY LID_ID";

            using var reader = await command.ExecuteReaderAsync();

            var rows = new List<(string, string, string, string)>();
            while (await reader.ReadAsync())
            {
                rows.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)));
            }

            rows.Should().BeEquivalentTo(CtsFixtures.ExpectedSteady,
                "the table is named for the dataset and its schema is inferred from the snapshot, so the load stage needed nothing adding");
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task ReadModel_IsUnchanged_ByTheDatasetBeingRegistered()
    {
        // The read model's SQL never names the dataset, so it is excluded by omission rather than by
        // anything the export stage does. What the export stage is asked to build is therefore the
        // comparison; the transformation itself is covered by DuckDbSqliteViewWriterTests, whose
        // fixtures carry the columns it reads.
        var withCts = new RecordingSqliteViewWriter();
        var withoutCts = new RecordingSqliteViewWriter();

        using (var host = InMemoryEtlPipelineHost.Create(
            CtsFixtures.RunClock, [.. EtlFixtures.AllThree, CtsFixtures.Definition], sqliteViewWriter: withCts))
        {
            await SeedLitprdAsync(host);
            await SeedSteadyAsync(host);

            await host.RunAsync(LookbackDays);
        }

        using (var host = InMemoryEtlPipelineHost.Create(
            CtsFixtures.RunClock, EtlFixtures.AllThree, sqliteViewWriter: withoutCts))
        {
            await SeedLitprdAsync(host);

            await host.RunAsync(LookbackDays);
        }

        using var scope = new AssertionScope();

        withCts.OnlyCall.Sql.Should().Be(withoutCts.OnlyCall.Sql);
        withCts.OnlyCall.TableNames.Should().BeEquivalentTo(withoutCts.OnlyCall.TableNames);
        withCts.OnlyCall.TableNames.Should().NotContain(CtsFixtures.Definition.Name,
            "the read model reads none of this dataset's columns, so it must not be asked for the table either");
    }

    [Fact]
    public async Task LitprdDatasets_AreUnaffected_ByTheDatasetBeingRegistered()
    {
        using var withCts = InMemoryEtlPipelineHost.Create(
            CtsFixtures.RunClock, [.. EtlFixtures.AllThree, CtsFixtures.Definition]);

        await SeedLitprdAsync(withCts);
        await SeedSteadyAsync(withCts);

        await withCts.RunAsync(LookbackDays);

        using var withoutCts = InMemoryEtlPipelineHost.Create(CtsFixtures.RunClock, EtlFixtures.AllThree);

        await SeedLitprdAsync(withoutCts);

        await withoutCts.RunAsync(LookbackDays);

        using var scope = new AssertionScope();

        foreach (var definition in EtlFixtures.AllThree)
        {
            var key = SnapshotFileNaming.SnapshotKey(definition, EtlFixtures.LatestSourceTimestamp);

            withCts.Folders.Folder(EtlPipelineFolders.Snapshots).Keys.Should().Contain(key,
                "{0} keeps its unhashed snapshot name: it has no baseline lane, so there is no hash to carry",
                definition.Name);

            withCts.Folders.Folder(EtlPipelineFolders.Snapshots).BytesOf(key).Should().Equal(
                withoutCts.Folders.Folder(EtlPipelineFolders.Snapshots).BytesOf(key),
                "and its snapshot is byte-identical to the one it produces with the dataset absent");
        }
    }

    /// <summary>Wide enough for one run to see both the litprd fixtures and the CTS ones, which sit
    /// nine months apart.</summary>
    private const int LookbackDays = 400;

    private static InMemoryEtlPipelineHost CreateHost(IStagingDatabaseWriter? writer = null)
        => InMemoryEtlPipelineHost.Create(CtsFixtures.RunClock, [CtsFixtures.Definition], writer);

    /// <summary>Scenario A's source files: bulk part 001 and the first four deltas, two of which carry
    /// nothing but a header.</summary>
    private static async Task SeedSteadyAsync(InMemoryEtlPipelineHost host)
    {
        (string Key, string Content)[] files =
        [
            (CtsFixtures.BulkPartOneKey, CtsFixtures.BulkPartOne),
            (CtsFixtures.FirstDeltaKey, CtsFixtures.FirstDelta),
            (CtsFixtures.SecondDeltaKey, CtsFixtures.SecondDelta),
            (CtsFixtures.ThirdDeltaKey, CtsFixtures.ThirdDelta),
            (CtsFixtures.FourthDeltaKey, CtsFixtures.FourthDelta)
        ];

        foreach (var (key, content) in files)
        {
            await PutAsync(host, key, content);
        }
    }

    private static Task<string> PutAsync(InMemoryEtlPipelineHost host, string key, string content)
        => host.PutEncryptedSourceFileAsync(key, content, PasswordDerivationPolicy.CtsDerived);

    private static async Task SeedLitprdAsync(InMemoryEtlPipelineHost host)
    {
        foreach (var definition in EtlFixtures.AllThree)
        {
            foreach (var (fileName, content) in EtlFixtures.FilesFor(definition))
            {
                await host.PutEncryptedSourceFileAsync(fileName, content);
            }
        }
    }

    private static async Task<List<(string Id, string Identifier, string Modified, string Version)>> RowsOf(
        InMemoryEtlPipelineHost host,
        string snapshotKey)
    {
        var rows = await SnapshotReader.ReadColumnsAsync(host, snapshotKey, CtsFixtures.AssertedColumns);

        return [.. rows.Select(row => (row[0]!, row[1]!, row[2]!, row[3]!))];
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
}
