using System.Security.Cryptography;
using FluentAssertions;
using KeeperData.Core.EtlPipeline.Views;
using KeeperData.Infrastructure.EtlPipeline.Views;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KeeperData.Infrastructure.Tests.Unit.EtlPipeline.Views;

/// <summary>The transformation itself, run for real against a DuckDB source and read back out of the
/// SQLite it produces. These are the acceptance criteria the read model has to keep meeting.</summary>
public sealed class DuckDbSqliteViewWriterTests : IDisposable
{
    private readonly string _workingDirectory =
        Directory.CreateTempSubdirectory("krds-view-tests-").FullName;

    private readonly string _sourcePath;

    public DuckDbSqliteViewWriterTests()
    {
        _sourcePath = Path.Combine(_workingDirectory, "staging.duckdb");
        SamExtractFixture.Create(_sourcePath);
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        try { Directory.Delete(_workingDirectory, recursive: true); } catch (IOException) { }
    }

    private static DuckDbSqliteViewWriter Sut(string? memoryLimit = null) => new(
        Options.Create(new DuckDbConfiguration
        {
            SqliteExtensionPath = DuckDbSqliteExtension.Path,
            MemoryLimit = memoryLimit
        }),
        NullLogger<DuckDbSqliteViewWriter>.Instance);

    private async Task<string> RunAsync(string name = "krds-db.sqlite")
    {
        var target = Path.Combine(_workingDirectory, name);

        await Sut().WriteAsync(
            new SqliteViewWriteRequest(
                _sourcePath, target, SqliteViewDefinition.Sql, SqliteViewDefinition.TableNames));

        return target;
    }

    [Fact]
    public async Task Produces_every_table_the_read_model_declares()
    {
        var target = await RunAsync();

        foreach (var table in SqliteViewDefinition.TableNames)
        {
            Scalar(target, $"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{table}'")
                .Should().Be(1L, "{0} is part of the read model", table);
        }
    }

    [Fact]
    public async Task Applies_the_configured_memory_limit()
    {
        var target = Path.Combine(_workingDirectory, "memory-limited.sqlite");

        var result = await Sut("512MB").WriteAsync(new SqliteViewWriteRequest(
            _sourcePath, target, SqliteViewDefinition.Sql, SqliteViewDefinition.TableNames));

        result.Tables.Should().HaveCount(SqliteViewDefinition.TableNames.Count);
    }

    [Fact]
    public async Task Counts_the_rows_it_wrote_into_each_table()
    {
        var target = Path.Combine(_workingDirectory, "counted.sqlite");

        var result = await Sut().WriteAsync(new SqliteViewWriteRequest(
            _sourcePath, target, SqliteViewDefinition.Sql, SqliteViewDefinition.TableNames));

        result.Tables.Should().BeEquivalentTo(new[]
        {
            new SqliteViewTable("Party", 6),
            new SqliteViewTable("Holding", 3),
            new SqliteViewTable("Herd", 1),
            new SqliteViewTable("HoldingAnimalProfile", 2),
            new SqliteViewTable("PartyRole", 5)
        });
    }

    [Fact]
    public async Task Builds_the_party_population_from_every_source_that_names_one()
    {
        var target = await RunAsync();

        // P1/P2/P3/P6 are named directly, P4 only holds CPHs, P5 only appears as a herd keeper token.
        Strings(target, "SELECT SourcePartyId FROM Party ORDER BY SourcePartyId")
            .Should().Equal("P1", "P2", "P3", "P4", "P5", "P6");
    }

    [Fact]
    public async Task Stores_missing_value_sentinels_as_nulls()
    {
        var target = await RunAsync();

        // P6 appears in no other extract, so '-', ',', '' and whitespace have nothing to give way to.
        Strings(target, "SELECT ifnull(PersonTitle,'<null>') || '|' || ifnull(GivenName,'<null>') || '|' || " +
                        "ifnull(FamilyName,'<null>') || '|' || ifnull(Telephone,'<null>') || '|' || " +
                        "ifnull(Email,'<null>') || '|' || ifnull(Roles,'<null>') " +
                        "FROM Party WHERE SourcePartyId='P6'")
            .Should().Equal(["<null>|<null>|<null>|<null>|<null>|<null>"]);
    }

    [Fact]
    public async Task Never_lets_a_sentinel_beat_a_real_value_from_the_other_extract()
    {
        var target = await RunAsync();

        // P4 is absent from sam_party entirely, so the holder extract is the only source of a name.
        Strings(target, "SELECT GivenName FROM Party WHERE SourcePartyId='P4'").Should().Equal(["Derek"]);

        // P2's names are sentinels in sam_party. A sentinel means absent, so the holder's real names
        // must win rather than being masked and normalised away to null.
        Strings(target, "SELECT GivenName || '|' || FamilyName FROM Party WHERE SourcePartyId='P2'")
            .Should().Equal(["Brenda|Baker"]);
    }

    [Fact]
    public async Task Creates_holdings_only_from_the_canonical_extract()
    {
        var target = await RunAsync();

        Strings(target, "SELECT Cph FROM Holding ORDER BY Cph")
            .Should().Equal("01/234/5678", "02/345/6789", "03/456/7890");
    }

    [Fact]
    public async Task Keeps_attributes_when_the_canonical_cph_needed_trimming()
    {
        var target = await RunAsync();

        Strings(target, "SELECT FeatureName || '|' || Town FROM Holding WHERE Cph='03/456/7890'")
            .Should().Equal(["Spaced Farm|Bodmin"]);
    }

    [Fact]
    public async Task Takes_holding_attributes_from_the_most_recent_source_record()
    {
        var target = await RunAsync();

        // Both records are current, so the date is a deterministic tie-break rather than a currency
        // rule - but the address must still follow the record the name came from.
        Strings(target, "SELECT FeatureName || '|' || Street FROM Holding WHERE Cph='01/234/5678'")
            .Should().Equal(["Main Farm|New Street"]);
    }

    [Fact]
    public async Task Falls_back_to_an_earlier_record_when_the_latest_name_is_a_placeholder()
    {
        var target = await RunAsync();

        Strings(target, "SELECT FeatureName FROM Holding WHERE Cph='02/345/6789'")
            .Should().Equal(["Known Farm"], "'Notknown' stands for absence, so it must not win");
    }

    [Fact]
    public async Task Normalises_the_organisation_name_placeholder_to_null()
    {
        var target = await RunAsync();

        Scalar(target, "SELECT count(*) FROM Party WHERE OrganisationName IS NOT NULL")
            .Should().Be(0L, "'No Organisation Name' must not read as an organisation");
    }

    [Fact]
    public async Task Normalises_the_casing_of_enum_like_values()
    {
        var target = await RunAsync();

        Strings(target, "SELECT CphType || '|' || UkInternalCode FROM Holding ORDER BY Cph")
            .Should().Equal("permanent|England", "temporary|Scotland", "emergency|Northern Ireland");
    }

    [Fact]
    public async Task Stores_dates_as_epoch_seconds()
    {
        var target = await RunAsync();

        Scalar(target, "SELECT StartDate FROM Holding WHERE Cph='01/234/5678'")
            .Should().Be(new DateTimeOffset(2025, 6, 1, 0, 0, 0, TimeSpan.Zero).ToUnixTimeSeconds());

        Scalar(target, "SELECT AnimalGroupFromDate FROM Herd WHERE Herdmark='AB1234'")
            .Should().Be(new DateTimeOffset(2008, 7, 16, 0, 0, 0, TimeSpan.Zero).ToUnixTimeSeconds());

        Strings(target, "SELECT typeof(StartDate) FROM Holding WHERE Cph='01/234/5678'")
            .Should().Equal(["integer"]);
    }

    [Fact]
    public async Task Leaves_the_end_of_an_open_record_null()
    {
        var target = await RunAsync();

        Scalar(target, "SELECT count(*) FROM Holding WHERE EndDate IS NOT NULL")
            .Should().Be(0L, "the extract carries only records that have not ended");
        Scalar(target, "SELECT count(*) FROM Herd WHERE AnimalGroupToDate IS NOT NULL")
            .Should().Be(0L);
    }

    [Fact]
    public async Task Never_materialises_a_holding_a_relationship_merely_mentions()
    {
        var target = await RunAsync();

        Scalar(target, "SELECT count(*) FROM Holding WHERE Cph='99/999/9999'")
            .Should().Be(0L, "a holder row referencing an unknown CPH must not create one");
    }

    [Fact]
    public async Task Excludes_herds_with_an_invalid_cphh_or_no_canonical_holding()
    {
        var target = await RunAsync();

        // CD5678 has a malformed CPHH; EF9012 is well formed but its CPH is not canonical.
        Strings(target, "SELECT Herdmark FROM Herd ORDER BY Herdmark").Should().Equal(["AB1234"]);
    }

    [Fact]
    public async Task Points_each_herd_at_the_holding_its_cphh_derives_from()
    {
        var target = await RunAsync();

        Strings(target, """
            SELECT h.Cph FROM Herd d JOIN Holding h ON h.Id = d.HoldingId WHERE d.Herdmark = 'AB1234'
            """)
            .Should().Equal(["01/234/5678"]);
    }

    [Fact]
    public async Task Deduplicates_animal_profiles_by_their_natural_key()
    {
        var target = await RunAsync();

        Scalar(target, """
            SELECT count(*)
            FROM HoldingAnimalProfile p
            JOIN Holding h ON h.Id = p.HoldingId
            WHERE h.Cph = '01/234/5678'
            """).Should().Be(1L);

        // The sentinel variants collapse, and '-' normalises away before the key is generated.
        Strings(target, "SELECT AnimalSpeciesCode || '|' || ifnull(AnimalProductionUsageCode,'<null>') " +
                        "FROM HoldingAnimalProfile WHERE AnimalSpeciesCode='01'")
            .Should().Equal(["01|<null>"]);
    }

    [Fact]
    public async Task Points_profiles_at_the_holding_for_the_trimmed_cph()
    {
        var target = await RunAsync();

        Strings(target, """
            SELECT h.Cph || '|' || p.AnimalSpeciesCode
            FROM HoldingAnimalProfile p
            JOIN Holding h ON h.Id = p.HoldingId
            WHERE p.AnimalSpeciesCode = '02'
            """).Should().Equal(["03/456/7890|02"]);

        Scalar(target, """
            SELECT count(*)
            FROM HoldingAnimalProfile p
            LEFT JOIN Holding h ON h.Id = p.HoldingId
            WHERE h.Id IS NULL
            """).Should().Be(0L);
    }

    [Fact]
    public async Task Creates_roles_only_where_party_holding_and_herd_all_resolve()
    {
        var target = await RunAsync();

        Strings(target, """
            SELECT p.SourcePartyId || '|' || h.Cph || '|' || r.Role
            FROM PartyRole r
            JOIN Party p ON p.Id = r.PartyId
            JOIN Holding h ON h.Id = r.HoldingId
            ORDER BY 1
            """)
            .Should().Equal(
                "P1|01/234/5678|keeper",
                "P1|01/234/5678|owner",
                "P2|01/234/5678|holder",
                "P2|02/345/6789|holder",
                "P5|01/234/5678|keeper");
    }

    [Fact]
    public async Task Leaves_herd_null_on_a_holding_level_holder_role()
    {
        var target = await RunAsync();

        Scalar(target, "SELECT count(*) FROM PartyRole WHERE Role='holder' AND HerdId IS NOT NULL")
            .Should().Be(0L);
        Scalar(target, "SELECT count(*) FROM PartyRole WHERE Role IN ('keeper','owner') AND HerdId IS NULL")
            .Should().Be(0L);
    }

    [Fact]
    public async Task Generates_uuid_shaped_ids_with_the_documented_version_and_variant()
    {
        var target = await RunAsync();

        Strings(target, "SELECT Id FROM Party")
            .Should().OnlyContain(id => IsVersion5Shaped(id));
    }

    [Fact]
    public async Task Generates_the_same_ids_for_the_same_snapshot()
    {
        var first = await RunAsync("first.sqlite");
        var second = await RunAsync("second.sqlite");

        foreach (var table in SqliteViewDefinition.TableNames)
        {
            Strings(first, $"SELECT Id FROM {table} ORDER BY Id")
                .Should().Equal(Strings(second, $"SELECT Id FROM {table} ORDER BY Id"),
                    "{0} ids must be stable across runs", table);
        }
    }

    [Fact]
    public async Task Leaves_the_source_database_untouched()
    {
        var before = Hash(_sourcePath);

        await RunAsync();

        Hash(_sourcePath).Should().Be(before, "the staging database is attached read-only");
    }

    [Fact]
    public async Task Creates_the_indexes_the_read_model_is_queried_through()
    {
        var target = await RunAsync();

        Strings(target, "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'ix_%' ORDER BY name")
            .Should().Equal(
                "ix_herd_holding",
                "ix_holding_cph",
                "ix_party_email",
                "ix_party_role_herd_role",
                "ix_party_role_holding_role",
                "ix_party_role_party_role");
    }

    [Fact]
    public async Task Produces_a_file_any_sqlite_client_can_open()
    {
        var target = await RunAsync();

        Strings(target, "PRAGMA integrity_check").Should().Equal(["ok"]);
    }

    [Fact]
    public async Task Refuses_to_overwrite_an_existing_export()
    {
        var target = await RunAsync();

        var act = async () => await Sut().WriteAsync(new SqliteViewWriteRequest(
            _sourcePath, target, SqliteViewDefinition.Sql, SqliteViewDefinition.TableNames));

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*already exists*");
    }

    [Fact]
    public async Task Rejects_a_blank_table_name_in_the_reporting_contract()
    {
        var act = async () => await Sut().WriteAsync(new SqliteViewWriteRequest(
            _sourcePath,
            Path.Combine(_workingDirectory, "blank-table.sqlite"),
            SqliteViewDefinition.Sql,
            [""]));

        await act.Should().ThrowAsync<ArgumentException>().WithMessage("Table name is required*");
    }

    [Fact]
    public async Task Explains_a_missing_bundled_extension_rather_than_reporting_an_io_error()
    {
        var writer = new DuckDbSqliteViewWriter(
            Options.Create(new DuckDbConfiguration
            {
                SqliteExtensionPath = Path.Combine(_workingDirectory, "not-bundled.duckdb_extension")
            }),
            NullLogger<DuckDbSqliteViewWriter>.Instance);

        var act = async () => await writer.WriteAsync(new SqliteViewWriteRequest(
            _sourcePath,
            Path.Combine(_workingDirectory, "never-written.sqlite"),
            SqliteViewDefinition.Sql,
            SqliteViewDefinition.TableNames));

        (await act.Should().ThrowAsync<SqliteViewExtensionException>())
            .Which.Message.Should().NotContain("not-bundled", "the configured path is logged, not served");
    }

    private static bool IsVersion5Shaped(string id)
        => Guid.TryParse(id, out _) && id[14] == '5' && id[19] == '8';

    private static long Scalar(string databasePath, string sql)
    {
        using var connection = Open(databasePath);
        using var command = connection.CreateCommand();
        command.CommandText = sql;

        return Convert.ToInt64(command.ExecuteScalar());
    }

    private static List<string> Strings(string databasePath, string sql)
    {
        using var connection = Open(databasePath);
        using var command = connection.CreateCommand();
        command.CommandText = sql;

        var values = new List<string>();
        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            values.Add(reader.GetString(0));
        }

        return values;
    }

    private static SqliteConnection Open(string databasePath)
    {
        var connection = new SqliteConnection($"Data Source={databasePath};Mode=ReadOnly");
        connection.Open();

        return connection;
    }

    private static string Hash(string path)
    {
        using var stream = File.OpenRead(path);

        return Convert.ToHexString(SHA256.HashData(stream));
    }
}
