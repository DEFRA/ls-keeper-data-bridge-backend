using DuckDB.NET.Data;
using FluentAssertions;
using FluentAssertions.Execution;
using KeeperData.Bridge.Tests.Integration.Helpers;
using KeeperData.Core.ETL.Impl;
using KeeperData.Core.EtlPipeline.Storage;
using Parquet;
using Xunit.Abstractions;

namespace KeeperData.Bridge.Tests.Integration.Scenarios;

/// <summary>
/// The multi-dataset end-to-end run, over LocalStack S3 with real crypto, real catalogue, real
/// Parquet and real DuckDB: source -> discover -> decrypt -> normalise -> snapshot -> load-duckdb.
///
/// The sibling class <see cref="EtlPipelineEndToEndTests"/> covers one dataset in depth. This one
/// covers three at once, which is where the different failures live: per-dataset prefix routing, one
/// staging database holding several tables, the dataset filter, and the two ingestion modes running
/// side by side in a single pass.
///
/// The three are chosen to span the axes: composite keys of four, three and one column. All twelve
/// definitions are currently DataSetIngestionMode.Delta, so every dataset folds its files.
///
/// One pipeline behaviour the fixtures must respect, deliberate in the code: Delta mode ignores D
/// rows (MergeState counts and skips them), so a deleted row survives at its previous value.
/// </summary>
[Collection("LocalStack"), Trait("Dependence", "docker")]
public sealed class EtlPipelineMultiDataSetTests(ITestOutputHelper output, LocalStackFixture localStack)
{
    private const string FirstStamp = "20251113121333";
    private const string SecondStamp = "20251114121333";
    private const string ThirdStamp = "20251115121333";

    private static readonly DateTimeOffset RunClock = new(2025, 11, 15, 18, 0, 0, TimeSpan.Zero);

    private static readonly DateTimeOffset LatestSourceTimestamp = new(2025, 11, 15, 12, 13, 33, TimeSpan.Zero);

    private static readonly DataSetDefinitions Standard = StandardDataSetDefinitionsBuilder.Build();

    private static DataSetDefinition CphHolding => Standard.SamCPHHolding;

    private static DataSetDefinition Herd => Standard.SamHerd;

    private static DataSetDefinition Party => Standard.SamParty;

    private static IReadOnlyList<DataSetDefinition> AllThree => [CphHolding, Herd, Party];

    // sam_cph_holdings, Delta mode. Four-column key, only CPH varies between rows.
    private const string CphHeader = "CPH|FEATURE_NAME|SECONDARY_CPH|ANIMAL_SPECIES_CODE|HOLDING_NAME|CHANGE_TYPE";
    private const string CphKeyTail = "MAIN|-|01";

    // sam_herd. Three-column key.
    private const string HerdHeader = "CPHH|HERDMARK|ANIMAL_PURPOSE_CODE|HERD_NAME|CHANGE_TYPE";

    // sam_party. Single-column key, the degenerate case worth covering explicitly.
    private const string PartyHeader = "PARTY_ID|PARTY_NAME|CHANGE_TYPE";

    [Fact]
    public async Task HappyPath_ThreeDataSets_FromEncryptedSourceToQueryableDatabase()
    {
        await using var host = await EtlPipelineTestHost.CreateAsync(localStack.S3Client, RunClock, AllThree);

        await SeedAllAsync(host);

        await host.RunPipelineAsync();

        await ReportFoldersAsync(host);

        using var scope = new AssertionScope();

        // 1. Decrypted. Nine files land in raw/ under their own names, content intact.
        (await host.ListFolderAsync(EtlPipelineFolders.Raw)).Should().HaveCount(9,
            "three dated files for each of the three datasets are decrypted");

        (await host.ReadTextAsync(EtlPipelineFolders.Raw, FileName(Party, ThirdStamp)))
            .Should().Be(PartyThird,
                "the decrypt stage derives the password from the object key, so the round trip must be exact");

        // 2. Normalised. Each dataset's Parquet files sit under its own prefix, not in one flat heap.
        var normalised = await host.ListFolderAsync(EtlPipelineFolders.Normalised);

        foreach (var definition in AllThree)
        {
            normalised
                .Where(key => key.StartsWith(SnapshotFileNaming.DataSetPrefix(definition), StringComparison.Ordinal))
                .Should().HaveCount(3, "every source file for {0} is normalised into its own prefix", definition.Name);
        }

        // 3. Snapshotted. One snapshot per dataset, holding what its ingestion mode dictates.
        (await host.ListFolderAsync(EtlPipelineFolders.Snapshots)).Should().Contain(
            [SnapshotKeyFor(CphHolding), SnapshotKeyFor(Herd), SnapshotKeyFor(Party)]);

        (await ReadSnapshotAsync(host, SnapshotKeyFor(CphHolding), "CPH", "HOLDING_NAME"))
            .Should().BeEquivalentTo(ExpectedCph,
                "sam_cph_holdings folds its three files; the D row is ignored by design");

        (await ReadSnapshotAsync(host, SnapshotKeyFor(Herd), "CPHH", "HERD_NAME"))
            .Should().BeEquivalentTo(ExpectedHerd,
                "sam_herd folds its three files, three-column key");

        (await ReadSnapshotAsync(host, SnapshotKeyFor(Party), "PARTY_ID", "PARTY_NAME"))
            .Should().BeEquivalentTo(ExpectedParty,
                "sam_party folds its three files, single-column key");

        // 4. Loaded. One database, three tables, every one queryable and complete.
        var databaseKey = StagingFileNaming.DatabaseKey(LatestSourceTimestamp);

        (await host.ListFolderAsync(EtlPipelineFolders.Staging)).Should().ContainSingle()
            .Which.Should().Be(databaseKey, "one database is produced, named for the newest source timestamp");

        var path = await host.DownloadToTempAsync(EtlPipelineFolders.Staging, databaseKey, ".duckdb");

        try
        {
            using var connection = new DuckDBConnection($"Data Source={path}");
            await connection.OpenAsync();

            (await QueryAsync(connection, "SELECT CPH, HOLDING_NAME FROM sam_cph_holdings"))
                .Should().BeEquivalentTo(ExpectedCph, "no row is lost loading sam_cph_holdings");

            (await QueryAsync(connection, "SELECT CPHH, HERD_NAME FROM sam_herd"))
                .Should().BeEquivalentTo(ExpectedHerd, "no row is lost loading sam_herd");

            (await QueryAsync(connection, "SELECT PARTY_ID, PARTY_NAME FROM sam_party"))
                .Should().BeEquivalentTo(ExpectedParty, "no row is lost loading sam_party");
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task DataSetFilter_RunsOnlyTheNamedDataSet()
    {
        await using var host = await EtlPipelineTestHost.CreateAsync(localStack.S3Client, RunClock, AllThree);

        await SeedAllAsync(host);

        await host.RunPipelineAsync(dataset: Herd.Name);

        (await host.ListFolderAsync(EtlPipelineFolders.Snapshots)).Should().BeEquivalentTo([SnapshotKeyFor(Herd)],
            "the filter is applied at the source stage, so the other datasets are never discovered");

        (await host.ListFolderAsync(EtlPipelineFolders.Raw)).Should().HaveCount(3,
            "only the named dataset's files are decrypted");
    }

    [Fact]
    public async Task SecondRun_WithNothingNew_ProducesNothingFurther()
    {
        await using var host = await EtlPipelineTestHost.CreateAsync(localStack.S3Client, RunClock, AllThree);

        await SeedAllAsync(host);

        await host.RunPipelineAsync();
        var afterFirst = await FolderStateAsync(host);

        await host.RunPipelineAsync();

        (await FolderStateAsync(host)).Should().BeEquivalentTo(afterFirst,
            "a re-run with no new source file is a no-op: every stage finds its output already present");
    }

    [Fact]
    public async Task LateArrivingFile_ForOneDataSet_MovesOnlyThatDataSetsSnapshot()
    {
        // The case a single-dataset test cannot reach: a second run that must advance one dataset
        // and leave the other two exactly where they were.
        await using var host = await EtlPipelineTestHost.CreateAsync(localStack.S3Client, RunClock, AllThree);

        await SeedAllAsync(host);
        await host.RunPipelineAsync();

        const string LateStamp = "20251116121333";
        var lateTimestamp = new DateTimeOffset(2025, 11, 16, 12, 13, 33, TimeSpan.Zero);

        await host.PutEncryptedSourceFileAsync(
            FileName(Party, LateStamp),
            Psv(PartyHeader, "P0000005|Dave Holder|I"));

        host.SetNow(new DateTimeOffset(2025, 11, 16, 18, 0, 0, TimeSpan.Zero));

        await host.RunPipelineAsync();

        var snapshots = await host.ListFolderAsync(EtlPipelineFolders.Snapshots);

        using var scope = new AssertionScope();

        snapshots.Should().Contain(SnapshotFileNaming.SnapshotKey(Party, lateTimestamp),
            "sam_party gained a newer file, so it gets a new snapshot");

        snapshots.Should().NotContain(SnapshotFileNaming.SnapshotKey(Herd, lateTimestamp),
            "sam_herd gained nothing, so its snapshot must not move");

        snapshots.Should().NotContain(SnapshotFileNaming.SnapshotKey(CphHolding, lateTimestamp),
            "sam_cph_holdings gained nothing either");

        (await ReadSnapshotAsync(host, SnapshotFileNaming.SnapshotKey(Party, lateTimestamp), "PARTY_ID", "PARTY_NAME"))
            .Should().BeEquivalentTo([.. ExpectedParty, ("P0000005", "Dave Holder")],
                "the late arrival folds onto the snapshot the first run produced, it does not replace it");
    }

    private static async Task SeedAllAsync(EtlPipelineTestHost host)
    {
        await host.PutEncryptedSourceFileAsync(FileName(CphHolding, FirstStamp), CphFirst);
        await host.PutEncryptedSourceFileAsync(FileName(CphHolding, SecondStamp), CphSecond);
        await host.PutEncryptedSourceFileAsync(FileName(CphHolding, ThirdStamp), CphThird);

        await host.PutEncryptedSourceFileAsync(FileName(Herd, FirstStamp), HerdFirst);
        await host.PutEncryptedSourceFileAsync(FileName(Herd, SecondStamp), HerdSecond);
        await host.PutEncryptedSourceFileAsync(FileName(Herd, ThirdStamp), HerdThird);

        await host.PutEncryptedSourceFileAsync(FileName(Party, FirstStamp), PartyFirst);
        await host.PutEncryptedSourceFileAsync(FileName(Party, SecondStamp), PartySecond);
        await host.PutEncryptedSourceFileAsync(FileName(Party, ThirdStamp), PartyThird);
    }

    private static string FileName(DataSetDefinition definition, string stamp)
        => $"{string.Format(definition.FilePrefixFormat, stamp)}.csv";

    private static string SnapshotKeyFor(DataSetDefinition definition)
        => SnapshotFileNaming.SnapshotKey(definition, LatestSourceTimestamp);

    // Delta mode. The D row in the third file is counted and skipped, so 01/001/0002 survives.
    private static string CphFirst => Psv(CphHeader,
        $"01/001/0001|{CphKeyTail}|Old Farm|I",
        $"01/001/0002|{CphKeyTail}|Keep Farm|I");

    private static string CphSecond => Psv(CphHeader,
        $"01/001/0001|{CphKeyTail}|Updated Farm|U",
        $"01/001/0003|{CphKeyTail}|New Farm|I");

    private static string CphThird => Psv(CphHeader,
        $"01/001/0002|{CphKeyTail}|Keep Farm|D");

    private static (string, string)[] ExpectedCph =>
    [
        ("01/001/0001", "Updated Farm"),
        ("01/001/0002", "Keep Farm"),
        ("01/001/0003", "New Farm")
    ];

    // Delta increments. The D row in each third file is counted and skipped, so its row survives.
    private static string HerdFirst => Psv(HerdHeader,
        "01/001/0001|AA1234|BR|Hill Herd|I",
        "01/001/0002|BB5678|DY|Vale Herd|I");

    private static string HerdSecond => Psv(HerdHeader,
        "01/001/0001|AA1234|BR|Hill Herd Renamed|U",
        "01/001/0004|CC9012|BR|Moor Herd|I");

    private static string HerdThird => Psv(HerdHeader,
        "01/001/0002|BB5678|DY|Vale Herd|D");

    private static (string, string)[] ExpectedHerd =>
    [
        ("01/001/0001", "Hill Herd Renamed"),
        ("01/001/0002", "Vale Herd"),
        ("01/001/0004", "Moor Herd")
    ];

    private static string PartyFirst => Psv(PartyHeader,
        "P0000001|Alice Holder|I",
        "P0000002|Bob Holder|I");

    private static string PartySecond => Psv(PartyHeader,
        "P0000001|Alice Renamed|U",
        "P0000003|Carol Holder|I");

    private static string PartyThird => Psv(PartyHeader,
        "P0000002|Bob Holder|D");

    private static (string, string)[] ExpectedParty =>
    [
        ("P0000001", "Alice Renamed"),
        ("P0000002", "Bob Holder"),
        ("P0000003", "Carol Holder")
    ];

    private static string Psv(string header, params string[] rows)
        => string.Join(Environment.NewLine, [header, .. rows]) + Environment.NewLine;

    /// <summary>Reads two named columns from a snapshot Parquet directly, so a load-stage defect
    /// cannot mask a merge-stage defect.</summary>
    private static async Task<List<(string, string)>> ReadSnapshotAsync(
        EtlPipelineTestHost host, string key, string keyColumn, string valueColumn)
    {
        var path = await host.DownloadToTempAsync(EtlPipelineFolders.Snapshots, key, ".parquet");

        try
        {
            await using var file = File.OpenRead(path);
            await using var reader = await ParquetReader.CreateAsync(file);

            var fields = reader.Schema.GetDataFields();
            var keyField = fields.Single(field => field.Name == keyColumn);
            var valueField = fields.Single(field => field.Name == valueColumn);

            var rows = new List<(string, string)>();

            for (var group = 0; group < reader.RowGroupCount; group++)
            {
                using var rowGroup = reader.OpenRowGroupReader(group);

                var keys = new string?[rowGroup.RowCount];
                var values = new string?[rowGroup.RowCount];

                await rowGroup.ReadAsync(keyField, keys.AsMemory());
                await rowGroup.ReadAsync(valueField, values.AsMemory());

                rows.AddRange(keys.Select((value, index) => (value!, values[index]!)));
            }

            return rows;
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static async Task<List<(string, string)>> QueryAsync(DuckDBConnection connection, string sql)
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

    private static async Task<Dictionary<string, IReadOnlyList<string>>> FolderStateAsync(EtlPipelineTestHost host)
    {
        string[] folders =
        [
            EtlPipelineFolders.Raw,
            EtlPipelineFolders.Normalised,
            EtlPipelineFolders.Snapshots,
            EtlPipelineFolders.Staging
        ];

        var state = new Dictionary<string, IReadOnlyList<string>>();

        foreach (var folder in folders)
        {
            state[folder] = await host.ListFolderAsync(folder);
        }

        return state;
    }

    /// <summary>Writes every folder's contents to the test output, so a failure in CI can be read
    /// without re-running against a bucket that no longer exists.</summary>
    private async Task ReportFoldersAsync(EtlPipelineTestHost host)
    {
        foreach (var (folder, keys) in await FolderStateAsync(host))
        {
            output.WriteLine($"{folder}/ ({keys.Count})");

            foreach (var key in keys)
            {
                output.WriteLine($"    {key}");
            }
        }
    }
}
