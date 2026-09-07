using DuckDB.NET.Data;
using FluentAssertions;
using FluentAssertions.Execution;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Staging;
using KeeperData.Core.EtlPipeline.Storage;
using KeeperData.Tests.SharedFixtures;
using Parquet;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Scenarios;

/// <summary>
/// The CTS location identifiers dataset over LocalStack S3, with real crypto, real Parquet and real
/// DuckDB: the same two scenarios the in-memory suite covers, over the sample's own files, run
/// against the storage the deployed service actually talks to.
///
/// What only this harness proves is the discovery half. The glob lists two prefixes of a real bucket
/// and has to come back with this table's files and none of the forty sibling tables', which is why
/// a sibling is seeded alongside the fixtures. The in-memory suite's storage double cannot tell the
/// difference.
///
/// Source files are encrypted with the password the extract uses - derived from the file name rather
/// than being it - so the derivation is exercised end to end rather than asserted in isolation.
/// </summary>
[Collection("LocalStack"), Trait("Dependence", "docker")]
public sealed class CtsLocationIdentifiersEndToEndTests(ITestOutputHelper output, LocalStackFixture localStack)
{
    private static readonly string SteadyHash = CtsFixtures.BaselineHashOf(CtsFixtures.BulkPartOneKey);

    private static readonly string ResetHash =
        CtsFixtures.BaselineHashOf(CtsFixtures.BulkPartOneKey, CtsFixtures.BulkPartTwoKey);

    private static readonly string SteadySnapshot =
        CtsFixtures.SnapshotKeyFor(CtsFixtures.FourthDeltaSourceTimestamp, SteadyHash);

    private static readonly string AdvancedSnapshot =
        CtsFixtures.SnapshotKeyFor(CtsFixtures.FifthDeltaSourceTimestamp, SteadyHash);

    private static readonly string ResetSnapshot =
        CtsFixtures.SnapshotKeyFor(CtsFixtures.FifthDeltaSourceTimestamp, ResetHash);

    [Fact]
    public async Task SteadyState_FoldsTheDeltasOntoTheBulk_AndResumesFromTheSnapshotItWrote()
    {
        await using var host = await CreateHostAsync();
        await SeedSteadyAsync(host);

        await host.RunPipelineAsync();

        using var scope = new AssertionScope();

        (await host.ListFolderAsync(EtlPipelineFolders.Raw)).Should().BeEquivalentTo(
            [
                CtsFixtures.BulkPartOneKey,
                CtsFixtures.FirstDeltaKey,
                CtsFixtures.SecondDeltaKey,
                CtsFixtures.ThirdDeltaKey,
                CtsFixtures.FourthDeltaKey
            ],
            "the glob finds this table's files in both lanes and leaves the sibling table alone");

        (await host.ReadTextAsync(EtlPipelineFolders.Raw, CtsFixtures.BulkPartOneKey)).Should().Be(
            CtsFixtures.BulkPartOne,
            "the file is encrypted with the password the extract derives from its name");

        (await host.ListFolderAsync(EtlPipelineFolders.Snapshots)).Should().BeEquivalentTo([SteadySnapshot],
            "the snapshot is named after the newest delta it includes, behind the bulk set's hash");

        (await ReadSnapshotAsync(host, SteadySnapshot)).Should().BeEquivalentTo(CtsFixtures.ExpectedSteady,
            "the delete is applied, the unrecognised audit type is not, and the header-only days change nothing");

        await host.RunPipelineAsync();

        (await host.ListFolderAsync(EtlPipelineFolders.Snapshots)).Should().BeEquivalentTo([SteadySnapshot],
            "a re-run with nothing new finds nothing newer than the snapshot's own name");

        await PutAsync(host, CtsFixtures.FifthDeltaKey, CtsFixtures.FifthDelta);

        await host.RunPipelineAsync();

        (await host.ListFolderAsync(EtlPipelineFolders.Snapshots)).Should().BeEquivalentTo(
            [SteadySnapshot, AdvancedSnapshot],
            "the bulk set has not moved, so the new delta folds onto the same lineage under a later name");

        (await ReadSnapshotAsync(host, AdvancedSnapshot)).Should().BeEquivalentTo(
            CtsFixtures.ExpectedSteadyAdvanced,
            "the twice-updated key takes the cut with the higher LID_AUD_ID, which the file carries second");

        var staged = await QueryDatabaseAsync(
            host, StagingFileNaming.DatabaseKey(CtsFixtures.FifthDeltaSourceTimestamp));

        staged.Should().BeEquivalentTo(CtsFixtures.ExpectedSteadyAdvanced,
            "the table is named for the dataset and its columns are inferred from the snapshot");

        output.WriteLine($"Snapshot and database agree on {staged.Count} row(s)");
    }

    [Fact]
    public async Task ChangedBulkSet_RebuildsFromEveryBulk_UnderANewKey()
    {
        await using var host = await CreateHostAsync();
        await SeedSteadyAsync(host);
        await PutAsync(host, CtsFixtures.FifthDeltaKey, CtsFixtures.FifthDelta);

        await host.RunPipelineAsync();

        await PutAsync(host, CtsFixtures.BulkPartTwoKey, CtsFixtures.BulkPartTwo);

        await host.RunPipelineAsync();

        using var scope = new AssertionScope();

        (await host.ListFolderAsync(EtlPipelineFolders.Snapshots)).Should().BeEquivalentTo(
            [AdvancedSnapshot, ResetSnapshot],
            "the reset lands on the timestamp the lineage it replaces already carries, so only the hash separates them");

        (await ReadSnapshotAsync(host, ResetSnapshot)).Should().BeEquivalentTo(CtsFixtures.ExpectedReset,
            "both parts are reprocessed and the deltas cut after them replay onto the rebuilt baseline");

        (await ReadSnapshotAsync(host, AdvancedSnapshot)).Should().BeEquivalentTo(
            CtsFixtures.ExpectedSteadyAdvanced,
            "and the snapshot the reset supersedes is untouched");
    }

    private Task<EtlPipelineTestHost> CreateHostAsync()
        => EtlPipelineTestHost.CreateAsync(localStack.S3Client, CtsFixtures.RunClock, CtsFixtures.Definition);

    /// <summary>Bulk part 001, the first four deltas - two of them header-only, as the sample's are -
    /// and one of the sibling tables sharing the lanes.</summary>
    private static async Task SeedSteadyAsync(EtlPipelineTestHost host)
    {
        (string Key, string Content)[] files =
        [
            (CtsFixtures.BulkPartOneKey, CtsFixtures.BulkPartOne),
            (CtsFixtures.FirstDeltaKey, CtsFixtures.FirstDelta),
            (CtsFixtures.SecondDeltaKey, CtsFixtures.SecondDelta),
            (CtsFixtures.ThirdDeltaKey, CtsFixtures.ThirdDelta),
            (CtsFixtures.FourthDeltaKey, CtsFixtures.FourthDelta),
            (CtsFixtures.SiblingTableKey, CtsFixtures.BulkPartOne)
        ];

        foreach (var (key, content) in files)
        {
            await PutAsync(host, key, content);
        }
    }

    private static Task<string> PutAsync(EtlPipelineTestHost host, string key, string content)
        => host.PutEncryptedSourceFileAsync(key, content, PasswordDerivationPolicy.CtsDerived);

    /// <summary>Reads a snapshot Parquet directly, so the assertion does not lean on the load stage.</summary>
    private static async Task<List<(string Id, string Identifier, string Modified, string Version)>> ReadSnapshotAsync(
        EtlPipelineTestHost host, string key)
    {
        var path = await host.DownloadToTempAsync(EtlPipelineFolders.Snapshots, key, ".parquet");

        try
        {
            await using var file = File.OpenRead(path);
            await using var reader = await ParquetReader.CreateAsync(file);

            var fields = reader.Schema.GetDataFields();

            fields.Select(field => field.Name).Should().BeEquivalentTo(CtsFixtures.SnapshotColumns,
                "the snapshot carries the bulk file's data shape: no audit lane, no RECORD_TYPE, no RECORD_COUNT");

            var selected = CtsFixtures.AssertedColumns
                .Select(name => fields.Single(field => field.Name == name))
                .ToArray();

            var rows = new List<(string, string, string, string)>();

            for (var group = 0; group < reader.RowGroupCount; group++)
            {
                using var rowGroup = reader.OpenRowGroupReader(group);

                var columns = new string?[selected.Length][];

                for (var column = 0; column < selected.Length; column++)
                {
                    columns[column] = new string?[rowGroup.RowCount];
                    await rowGroup.ReadAsync(selected[column], columns[column].AsMemory());
                }

                for (var row = 0; row < rowGroup.RowCount; row++)
                {
                    rows.Add((columns[0][row]!, columns[1][row]!, columns[2][row]!, columns[3][row]!));
                }
            }

            return rows;
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static async Task<List<(string Id, string Identifier, string Modified, string Version)>> QueryDatabaseAsync(
        EtlPipelineTestHost host, string key)
    {
        var path = await host.DownloadToTempAsync(EtlPipelineFolders.Staging, key, ".duckdb");

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

            return rows;
        }
        finally
        {
            File.Delete(path);
        }
    }
}
